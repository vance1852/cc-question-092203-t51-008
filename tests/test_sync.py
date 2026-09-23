"""终端离线事件批量同步测试。

覆盖：稳定标识/代次/序号、乱序与缺口、重传幂等、冲突人工裁决、
缺号跳过、旧事件不倒写在线电量、设备重置代次轮转、重启恢复与对账不变量。
"""
import uuid
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.database import SessionLocal
from app.main import app
from app.models import SwapRecord, SyncEvent
from app.seed import init_db
from app.services import sync_service

init_db()
client = TestClient(app)


def _login() -> str:
    resp = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def _headers() -> dict:
    return {"Authorization": f"Bearer {_login()}"}


def _new_vehicle(headers, soc=10.0, plate=None):
    plate = plate or f"测SYNC{uuid.uuid4().hex[:6]}"
    return client.post(
        "/api/vehicles", headers=headers, json={"plate": plate, "current_soc": soc}
    ).json()


def _new_station(headers, battery_ready=5, slot_total=10):
    return client.post(
        "/api/stations",
        headers=headers,
        json={
            "name": f"山区站{uuid.uuid4().hex[:6]}",
            "slot_total": slot_total,
            "battery_ready": battery_ready,
        },
    ).json()


def _event(event_id, seq, station_id, vehicle_id, *, soc_before=5.0, soc_after=100.0,
           occurred_at="2026-09-20T08:00:00", vehicle_plate=None):
    data = {
        "event_id": event_id,
        "seq": seq,
        "station_id": station_id,
        "soc_before": soc_before,
        "soc_after": soc_after,
        "occurred_at": occurred_at,
    }
    if vehicle_id is not None:
        data["vehicle_id"] = vehicle_id
    if vehicle_plate is not None:
        data["vehicle_plate"] = vehicle_plate
    return data


def _batch(headers, device, gen, events):
    return client.post(
        "/api/sync/batch",
        headers=headers,
        json={"device_id": device, "session_generation": gen, "events": events},
    )


# ---------------------------------------------------------------------------
# 基本顺序、乱序、缺口
# ---------------------------------------------------------------------------

def test_out_of_order_gap_then_autodrain():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h)
    veh = _new_vehicle(h)

    # 先到 seq2：缺前序 -> waiting
    r = _batch(h, dev, 1, [_event("e2", 2, st["id"], veh["id"])])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["confirmed_seq"] == 0
    assert body["results"][0]["status"] == "waiting"
    assert body["blocked_since"] is not None

    # 车辆/库存此时不应变化
    s0 = client.get(f"/api/stations/{st['id']}", headers=h).json()
    assert s0["battery_ready"] == st["battery_ready"]

    # 补齐 seq1：seq1 立即应用，队首随后自动推进 seq2
    r = _batch(h, dev, 1, [_event("e1", 1, st["id"], veh["id"])])
    body = r.json()
    assert body["confirmed_seq"] == 2
    assert body["blocked_since"] is None
    assert body["results"][0]["status"] == "applied"

    # 后续查询可见 seq2 也已 applied
    evs = client.get(f"/api/sync/devices/{dev}/events", headers=h).json()
    assert {e["seq"]: e["status"] for e in evs} == {1: "applied", 2: "applied"}

    s1 = client.get(f"/api/stations/{st['id']}", headers=h).json()
    assert s1["battery_ready"] == st["battery_ready"] - 2
    v1 = client.get(f"/api/vehicles/{veh['id']}", headers=h).json()
    assert v1["current_soc"] == 100.0


def test_batch_requires_sequence_and_identity_fields():
    h = _headers()
    # 缺少车辆标识 / 非法序号 / 空批次应被 422 拦截
    r = _batch(h, "dev-x", 1, [{
        "event_id": "z", "seq": 1, "station_id": 1,
        "soc_before": 1, "soc_after": 2,
    }])
    assert r.status_code == 422
    r = _batch(h, "dev-x", 1, [{
        "event_id": "z", "seq": 0, "station_id": 1, "vehicle_id": 1,
        "soc_before": 1, "soc_after": 2,
    }])
    assert r.status_code == 422
    r = client.post("/api/sync/batch", headers=h,
                    json={"device_id": "dev-x", "session_generation": 1, "events": []})
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# 重传幂等：库存至多扣一次
# ---------------------------------------------------------------------------

def test_replay_whole_batch_is_idempotent_inventory_once():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h, battery_ready=3)
    veh = _new_vehicle(h)
    events = [
        _event("p1", 1, st["id"], veh["id"], soc_after=90.0),
        _event("p2", 2, st["id"], veh["id"], soc_after=100.0),
    ]

    first = _batch(h, dev, 1, events).json()
    assert first["confirmed_seq"] == 2
    ready_after_first = client.get(f"/api/stations/{st['id']}", headers=h).json()["battery_ready"]
    assert ready_after_first == 1  # 5? no: created with 3 -> 3-2=1

    # 整批原样重发多次：全部 duplicate，库存与电量不再变化
    for _ in range(3):
        body = _batch(h, dev, 1, events).json()
        assert {r["status"] for r in body["results"]} == {"duplicate"}
        assert body["confirmed_seq"] == 2
        ready = client.get(f"/api/stations/{st['id']}", headers=h).json()["battery_ready"]
        assert ready == ready_after_first

    db = SessionLocal()
    try:
        # 每个事件仅对应一条换电记录
        rows = db.query(SwapRecord).filter(SwapRecord.sync_event_id.in_(["p1", "p2"])).all()
        assert len(rows) == 2
    finally:
        db.close()


def test_concurrent_duplicate_batches_apply_once():
    """同一批并发重发，库存仍只扣一次。"""
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h, battery_ready=10)
    veh = _new_vehicle(h)
    events = [_event("c1", 1, st["id"], veh["id"])]

    def send():
        # 每个线程独立登录拿 token，模拟终端重发
        tok = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"}).json()["access_token"]
        return client.post("/api/sync/batch", headers={"Authorization": f"Bearer {tok}"},
                           json={"device_id": dev, "session_generation": 1, "events": events}).json()

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: send(), range(6)))

    statuses = sorted(r["results"][0]["status"] for r in results)
    assert statuses.count("applied") == 1
    assert statuses.count("duplicate") == 5
    ready = client.get(f"/api/stations/{st['id']}", headers=h).json()["battery_ready"]
    assert ready == 9


# ---------------------------------------------------------------------------
# 业务冲突与人工裁决
# ---------------------------------------------------------------------------

def test_conflict_requires_manual_reason_and_resumes_queue():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h, battery_ready=0)  # 无库存 -> 冲突
    veh = _new_vehicle(h)

    body = _batch(h, dev, 1, [
        _event("k1", 1, st["id"], veh["id"]),
        _event("k2", 2, st["id"], veh["id"], soc_after=95.0),
    ]).json()
    assert body["confirmed_seq"] == 0
    assert body["results"][0]["status"] == "conflict"
    assert body["results"][1]["status"] == "waiting"

    # 裁决必须留原因
    no_reason = client.post(f"/api/sync/devices/{dev}/events/k1/resolve", headers=h,
                            json={"action": "reject", "reason": ""})
    assert no_reason.status_code == 422

    # 库存仍为 0 时不能强制扣减
    busy = client.post(f"/api/sync/devices/{dev}/events/k1/resolve", headers=h,
                       json={"action": "force_apply", "reason": "试试"})
    assert busy.status_code == 409

    # 补货后强制应用，必须留原因与载荷摘要；随后队列自动推进到 k2
    client.put(f"/api/stations/{st['id']}", headers=h, json={"battery_ready": 2})
    r = client.post(f"/api/sync/devices/{dev}/events/k1/resolve", headers=h, json={
        "action": "force_apply", "reason": "现场监控确认换电真实发生，站点已补货",
    })
    assert r.status_code == 200, r.text
    resolved = r.json()
    assert resolved["status"] == "applied"
    assert resolved["resolved_by"] == "admin"
    assert "现场监控" in resolved["resolution_reason"]
    assert len(resolved["payload_hash"]) == 64  # 原始载荷摘要留存
    assert resolved["swap_record_id"] is not None

    evs = client.get(f"/api/sync/devices/{dev}/events", headers=h).json()
    assert {e["seq"]: e["status"] for e in evs} == {1: "applied", 2: "applied"}

    # 驳回路径：软前提冲突（soc_after <= soc_before），驳回后续推
    st2 = _new_station(h, battery_ready=2)
    body = _batch(h, dev + "b", 1, [
        _event("m1", 1, st2["id"], veh["id"], soc_before=90, soc_after=50),
        _event("m2", 2, st2["id"], veh["id"], soc_after=99),
    ]).json()
    assert body["results"][0]["status"] == "conflict"
    rej = client.post(f"/api/sync/devices/{dev}b/events/m1/resolve", headers=h,
                      json={"action": "reject", "reason": "终端采集异常，驳回待重传"}).json()
    assert rej["status"] == "rejected"
    assert rej["resolved_by"] == "admin"
    after = {e["seq"]: e["status"] for e in
             client.get(f"/api/sync/devices/{dev}b/events", headers=h).json()}
    assert after == {1: "rejected", 2: "applied"}


def test_cannot_resolve_non_conflict():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h)
    veh = _new_vehicle(h)
    _batch(h, dev, 1, [_event("q1", 1, st["id"], veh["id"])]).json()
    r = client.post(f"/api/sync/devices/{dev}/events/q1/resolve", headers=h,
                    json={"action": "reject", "reason": "已应用不该被裁决"})
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# 缺号跳过
# ---------------------------------------------------------------------------

def test_skip_gap_tombstone_resumes_queue():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h)
    veh = _new_vehicle(h)

    body = _batch(h, dev, 1, [_event("g3", 3, st["id"], veh["id"])]).json()
    assert body["results"][0]["status"] == "waiting"

    # 不能跳过一个真实存在的序号
    bad = client.post(f"/api/sync/devices/{dev}/skip-gap", headers=h,
                      json={"seq": 3, "reason": "x"})
    assert bad.status_code == 422

    for seq in (1, 2):
        r = client.post(f"/api/sync/devices/{dev}/skip-gap", headers=h,
                        json={"seq": seq, "reason": f"终端确认序号 {seq} 掉电丢失"})
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "skipped_gap"

    # 缺号补齐为墓碑后，seq3 自动应用
    evs = client.get(f"/api/sync/devices/{dev}/events", headers=h).json()
    statuses = {e["seq"]: e["status"] for e in evs}
    assert statuses == {1: "skipped_gap", 2: "skipped_gap", 3: "applied"}
    cursor = client.get(f"/api/sync/devices/{dev}/cursor", headers=h).json()
    assert cursor["applied_seq"] == 3
    assert cursor["blocked_since"] is None


# ---------------------------------------------------------------------------
# 旧离线事件不倒写在线电量
# ---------------------------------------------------------------------------

def test_stale_offline_event_does_not_overwrite_newer_soc():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h, battery_ready=5)
    veh = _new_vehicle(h, soc=20.0)

    # 在线交易先发生（业务时间=当前），电量到 95
    online = client.post("/api/swaps", headers=h, json={
        "vehicle_id": veh["id"], "station_id": st["id"],
        "soc_before": 20, "soc_after": 95,
    })
    assert online.status_code == 201
    ready_after_online = client.get(f"/api/stations/{st['id']}", headers=h).json()["battery_ready"]

    # 晚到的旧离线事件（业务时间更早）声称电量应为 100
    body = _batch(h, dev, 1, [
        _event("old1", 1, st["id"], veh["id"], soc_after=100.0,
               occurred_at="2026-09-10T08:00:00")
    ]).json()
    applied = body["results"][0]
    assert applied["status"] == "applied"  # 仍记账、仍扣库存
    assert applied["swap_record_id"] is not None

    # 车辆电量保持在线交易后的 95，未被倒写
    v = client.get(f"/api/vehicles/{veh['id']}", headers=h).json()
    assert v["current_soc"] == 95.0
    # 库存只扣一次
    s = client.get(f"/api/stations/{st['id']}", headers=h).json()
    assert s["battery_ready"] == ready_after_online - 1

    ev = client.get(f"/api/sync/devices/{dev}/events", headers=h).json()[0]
    assert ev["soc_write_suppressed"] is True
    assert ev["detail"]


def test_newer_offline_event_does_update_soc():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h)
    veh = _new_vehicle(h)
    # 业务时间为未来（相对种子/在线），正常回写
    body = _batch(h, dev, 1, [
        _event("fresh1", 1, st["id"], veh["id"], soc_after=88.0,
               occurred_at="2026-12-01T08:00:00")
    ]).json()
    assert body["results"][0]["status"] == "applied"
    ev = client.get(f"/api/sync/devices/{dev}/events", headers=h).json()[0]
    assert ev["soc_write_suppressed"] is False
    v = client.get(f"/api/vehicles/{veh['id']}", headers=h).json()
    assert v["current_soc"] == 88.0


# ---------------------------------------------------------------------------
# 设备重置 / 会话代次轮转
# ---------------------------------------------------------------------------

def test_session_generation_rotation_rejects_stale_and_restarts_sequence():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h)
    veh = _new_vehicle(h)

    # 代次 1：seq2 等待（seq1 缺）
    _batch(h, dev, 1, [_event("gen1-2", 2, st["id"], veh["id"])]).json()

    # 设备重置，代次 2 从 seq1 开始
    body = _batch(h, dev, 2, [_event("gen2-1", 1, st["id"], veh["id"])]).json()
    assert body["session_generation"] == 2
    assert body["confirmed_seq"] == 1
    cursor = client.get(f"/api/sync/devices/{dev}/cursor", headers=h).json()
    assert cursor["session_generation"] == 2

    # 旧代次挂起事件被终结留痕
    old = client.get(f"/api/sync/devices/{dev}/events?generation=1", headers=h).json()
    assert old[0]["status"] == "rejected"
    assert old[0]["resolution_reason"] == "会话代次轮转"

    # 旧代次的迟到事件：拒收且持久化（重传仍 rejected，不入队）
    late = _batch(h, dev, 1, [_event("gen1-9", 9, st["id"], veh["id"])]).json()
    assert late["session_generation"] == 2
    assert late["results"][0]["status"] == "rejected"
    assert "旧会话代次" in late["results"][0]["detail"]
    again = _batch(h, dev, 1, [_event("gen1-9", 9, st["id"], veh["id"])]).json()
    assert again["results"][0]["status"] == "rejected"


def test_unstable_event_id_or_coordinates_rejected():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h)
    veh = _new_vehicle(h)
    _batch(h, dev, 1, [_event("stable-1", 1, st["id"], veh["id"])]).json()

    # 同一序号携带不同 event_id
    body = _batch(h, dev, 1, [_event("other-id", 1, st["id"], veh["id"])]).json()
    assert body["results"][0]["status"] == "rejected"

    # event_id 复用到别的序号
    body = _batch(h, dev, 1, [_event("stable-1", 2, st["id"], veh["id"])]).json()
    assert body["results"][0]["status"] == "rejected"

    # 原事件不受影响、库存仍只扣一次
    evs = client.get(f"/api/sync/devices/{dev}/events", headers=h).json()
    assert len(evs) == 1 and evs[0]["status"] == "applied"


# ---------------------------------------------------------------------------
# 游标查询、重启恢复
# ---------------------------------------------------------------------------

def test_cursor_query_404_for_unknown_device():
    h = _headers()
    r = client.get(f"/api/sync/devices/unknown-{uuid.uuid4().hex}/cursor", headers=h)
    assert r.status_code == 404


def test_restart_recovers_pending_queue_from_persisted_state():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h)
    veh = _new_vehicle(h)

    # 缺口状态：seq2 waiting（持久化）
    _batch(h, dev, 1, [_event("rc2", 2, st["id"], veh["id"])]).json()

    # 模拟服务重启：直接用新会话执行启动恢复
    db = SessionLocal()
    try:
        report = sync_service.recover_pending(db)
    finally:
        db.close()
    # seq1 仍缺，恢复后仍在等待
    cursor = client.get(f"/api/sync/devices/{dev}/cursor", headers=h).json()
    assert cursor["applied_seq"] == 0
    assert cursor["blocked_since"] is not None

    # 缺口在重启后补齐：恢复逻辑应自动应用 seq1、seq2
    _batch(h, dev, 1, [_event("rc1", 1, st["id"], veh["id"])]).json()
    db = SessionLocal()
    try:
        sync_service.recover_pending(db)
    finally:
        db.close()
    evs = client.get(f"/api/sync/devices/{dev}/events", headers=h).json()
    assert {e["seq"]: e["status"] for e in evs} == {1: "applied", 2: "applied"}


# ---------------------------------------------------------------------------
# 对账
# ---------------------------------------------------------------------------

def test_reconcile_invariants():
    h = _headers()
    rep = client.get("/api/sync/reconcile", headers=h)
    assert rep.status_code == 200
    data = rep.json()
    assert data["inventory_effect_at_most_once"] is True
    assert data["every_applied_has_swap"] is True
    assert data["online_not_overwritten_by_stale"] is True
    assert data["overwrite_violations"] == []
    # 已应用事件数与离线换电记录行数严格相等
    assert data["applied_count"] == data["offline_swap_row_count"]

    db = SessionLocal()
    try:
        # 数据库层唯一约束兜底：同一 event_id 无法产生两条换电记录
        event = db.query(SyncEvent).filter(SyncEvent.status == "applied").first()
        assert event is not None
        dup = SwapRecord(
            vehicle_id=event.swap_record.vehicle_id,
            station_id=event.swap_record.station_id,
            soc_before=1, soc_after=2,
            sync_event_id=event.event_id,
        )
        db.add(dup)
        import sqlalchemy.exc
        try:
            db.commit()
            raised = False
        except sqlalchemy.exc.IntegrityError:
            db.rollback()
            raised = True
        assert raised
    finally:
        db.close()


def test_plate_only_event_resolves_vehicle():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    st = _new_station(h)
    veh = _new_vehicle(h)
    body = _batch(h, dev, 1, [_event(
        "plate1", 1, st["id"], None, vehicle_plate=veh["plate"]
    )]).json()
    assert body["results"][0]["status"] == "applied"


def test_unknown_entities_rejected_not_crash():
    h = _headers()
    dev = f"dev-{uuid.uuid4().hex[:8]}"
    body = _batch(h, dev, 1, [_event("bad-station", 1, 9_999_999, 1)]).json()
    assert body["results"][0]["status"] == "rejected"
    body = _batch(h, dev + "2", 1, [_event(
        "bad-veh", 1, _new_station(h)["id"], 9_999_998
    )]).json()
    assert body["results"][0]["status"] == "rejected"
