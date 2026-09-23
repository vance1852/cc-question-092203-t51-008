"""离线批量同步端到端测试。

覆盖：顺序应用、乱序/缺口自动续推、整批重传幂等、设备代次重置、
同序号事件标识冲突、业务冲突与人工解决（原因留痕）、在线交易防旧事件倒写、
服务重启不丢队列、同设备并发提交不重复扣库存、对账证明。
"""
import threading
import uuid
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from app.database import SessionLocal
from app.main import app
from app.models import DeviceCursor, OfflineEvent
from app.seed import init_db
from app.services.sync import pump_all_devices

init_db()
client = TestClient(app)


def _login() -> str:
    resp = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def _auth() -> dict:
    return {"Authorization": f"Bearer {_login()}"}


def _make_vehicle(headers: dict, soc: float = 20.0) -> int:
    plate = f"测{uuid.uuid4().hex[:8]}"
    resp = client.post(
        "/api/vehicles",
        json={"plate": plate, "model": "离线同步测试车", "current_soc": soc},
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


def _station_with_batteries(headers: dict, count: int = 10) -> dict:
    resp = client.post(
        "/api/stations",
        json={
            "name": f"山区站{uuid.uuid4().hex[:6]}",
            "address": "山路 1 号",
            "slot_total": count,
            "battery_ready": count,
        },
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _event(seq: int, vehicle_id: int, station_id: int, *, uid=None, occurred_at=None,
           soc_before=20.0, soc_after=100.0) -> dict:
    return {
        "event_uid": uid or f"ev-{uuid.uuid4().hex}",
        "seq": seq,
        "vehicle_id": vehicle_id,
        "station_id": station_id,
        "soc_before": soc_before,
        "soc_after": soc_after,
        "occurred_at": (occurred_at or datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
    }


def _batch(device_id: str, events: list[dict], *, generation="gen-1", started_at=None, station_id=None) -> dict:
    return {
        "device_id": device_id,
        "generation": generation,
        "generation_started_at": (started_at or datetime(2026, 9, 20, tzinfo=timezone.utc)).isoformat(),
        "station_id": station_id,
        "events": events,
    }


def _by_seq(results: list[dict]) -> dict[int, dict]:
    return {r["seq"]: r for r in results}


# ---------- 1. 乱序、缺口、补齐后自动续推 ----------
def test_out_of_order_then_gap_fill_auto_advances():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v1, v2, v3 = _make_vehicle(h), _make_vehicle(h), _make_vehicle(h)
    ready0 = station["battery_ready"]

    # 先到 seq=3：只能挂起
    r3 = client.post("/api/sync/batches", json=_batch(dev, [
        _event(3, v3, station["id"], occurred_at=datetime(2026, 9, 21, 10, 3, tzinfo=timezone.utc)),
    ]), headers=h)
    assert r3.status_code == 200, r3.text
    assert _by_seq(r3.json()["results"])[3]["status"] == "waiting"
    assert r3.json()["confirmed_seq"] == 0

    # 游标确认范围：缺口 [1,2]
    cur = client.get(f"/api/sync/devices/{dev}", headers=h).json()
    assert cur["confirmed_seq"] == 0 and cur["missing_seqs"] == [1, 2]

    # 再到 seq=1：应用 1，seq=2 仍缺，seq=3 保持等待
    r1 = client.post("/api/sync/batches", json=_batch(dev, [
        _event(1, v1, station["id"], occurred_at=datetime(2026, 9, 21, 10, 1, tzinfo=timezone.utc)),
    ]), headers=h)
    assert _by_seq(r1.json()["results"])[1]["status"] == "applied"
    assert r1.json()["confirmed_seq"] == 1
    cur = client.get(f"/api/sync/devices/{dev}", headers=h).json()
    assert cur["missing_seqs"] == [2]

    # 补齐 seq=2：闸门自动连续应用 2、3（历史等待事件借本次补齐生效）
    r2 = client.post("/api/sync/batches", json=_batch(dev, [
        _event(2, v2, station["id"], occurred_at=datetime(2026, 9, 21, 10, 2, tzinfo=timezone.utc)),
    ]), headers=h)
    body = r2.json()
    assert body["confirmed_seq"] == 3
    assert _by_seq(body["results"])[2]["status"] == "applied"

    # 三笔各扣一次库存，共扣 3
    s_now = client.get(f"/api/stations/{station['id']}", headers=h).json()
    assert s_now["battery_ready"] == ready0 - 3
    # 车辆电量使用各自事件的结果
    assert client.get(f"/api/vehicles/{v3}", headers=h).json()["current_soc"] == 100.0


# ---------- 2. 整批安全重发：全部 duplicate，库存不重复扣 ----------
def test_full_batch_resend_is_idempotent():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v = _make_vehicle(h)
    ev = _event(1, v, station["id"])
    batch = _batch(dev, [ev])

    first = client.post("/api/sync/batches", json=batch, headers=h)
    assert first.json()["applied_count"] == 1
    swap_id = first.json()["results"][0]["swap_record_id"]
    ready_after_first = client.get(f"/api/stations/{station['id']}", headers=h).json()["battery_ready"]

    # 终端原样重发整批
    second = client.post("/api/sync/batches", json=batch, headers=h)
    body = second.json()
    assert body["applied_count"] == 0 and body["duplicate_count"] == 1
    assert body["results"][0]["status"] == "duplicate"
    assert body["results"][0]["swap_record_id"] == swap_id  # 指向同一笔换电记录

    ready_after_second = client.get(f"/api/stations/{station['id']}", headers=h).json()["battery_ready"]
    assert ready_after_second == ready_after_first  # 没有第二次扣库存
    # 该车辆的离线换电记录只有 1 条
    swaps = client.get("/api/swaps", headers=h).json()
    offline_for_v = [s for s in swaps if s["vehicle_id"] == v]
    assert len(offline_for_v) == 1


# ---------- 3. 同事件标识改载荷重传：冲突且绝不第二次生效 ----------
def test_same_uid_altered_payload_rejected():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v = _make_vehicle(h)
    ev = _event(1, v, station["id"])
    client.post("/api/sync/batches", json=_batch(dev, [ev]), headers=h)

    tampered = dict(ev, soc_before=5.0)
    resp = client.post("/api/sync/batches", json=_batch(dev, [tampered]), headers=h)
    assert resp.json()["results"][0]["status"] == "conflict"
    assert "不一致" in resp.json()["results"][0]["reason"]


# ---------- 4. 同序号不同事件标识（设备重置后序号重用但未换代次）：冲突 ----------
def test_same_seq_different_uid_conflicts():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v1, v2 = _make_vehicle(h), _make_vehicle(h)
    e1 = _event(1, v1, station["id"], uid="uid-A")
    e2 = _event(1, v2, station["id"], uid="uid-B")
    client.post("/api/sync/batches", json=_batch(dev, [e1]), headers=h)
    resp = client.post("/api/sync/batches", json=_batch(dev, [e2]), headers=h)
    r = resp.json()["results"][0]
    assert r["status"] == "conflict"
    assert "uid-A" in r["reason"]
    # 只有首传事件生效，库存只扣一次
    ready = client.get(f"/api/stations/{station['id']}", headers=h).json()["battery_ready"]
    assert ready == station["battery_ready"] - 1


# ---------- 5. 业务冲突阻断顺序推进，人工 drop 后自动续推 ----------
def test_business_conflict_blocks_and_manual_drop_resumes():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h, count=2)
    v1, v2, v3 = _make_vehicle(h), _make_vehicle(h), _make_vehicle(h)
    t = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)

    # seq1、seq2 正常，耗尽到只剩 0 块电池（站点初始 2）
    client.post("/api/sync/batches", json=_batch(dev, [
        _event(1, v1, station["id"], occurred_at=t),
        _event(2, v2, station["id"], occurred_at=t + timedelta(minutes=1)),
        _event(3, v3, station["id"], occurred_at=t + timedelta(minutes=2)),
    ]), headers=h)
    # seq3 因无电池冲突，阻断；confirmed 停在 2
    cur = client.get(f"/api/sync/devices/{dev}", headers=h).json()
    assert cur["confirmed_seq"] == 2 and cur["has_open_conflict"] is True

    conflicts = client.get("/api/sync/events", params={"device_id": dev, "status_filter": "conflict"}, headers=h).json()
    assert len(conflicts) == 1
    conflict = conflicts[0]
    assert conflict["seq"] == 3 and "库存" in conflict["conflict_reason"]
    assert len(conflict["payload_sha256"]) == 64 and conflict["payload_json"]

    # 解决原因必填
    bad = client.post(f"/api/sync/events/{conflict['id']}/resolve",
                      json={"action": "drop", "reason": ""}, headers=h)
    assert bad.status_code == 422

    # 人工丢弃（例如站点补电后该笔已线下另处理），后续应可续推（此处 seq3 即末笔）
    ok = client.post(f"/api/sync/events/{conflict['id']}/resolve",
                     json={"action": "drop", "reason": "经电话核实该笔换电已在邻站完成，丢弃"}, headers=h)
    assert ok.status_code == 200, ok.text
    resolved = ok.json()
    assert resolved["resolved_by"] == "admin" and resolved["resolution_action"] == "dropped"
    assert "人工处理" in resolved["conflict_reason"]

    cur = client.get(f"/api/sync/devices/{dev}", headers=h).json()
    assert cur["confirmed_seq"] == 3 and cur["has_open_conflict"] is False
    rec = client.get(f"/api/sync/events/{conflict['id']}", headers=h).json()
    assert rec["status"] == "resolved" and rec["resolved_at"]


# ---------- 6. 人工 apply 解决业务冲突（库存补充后补应用） ----------
def test_manual_apply_after_restock():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h, count=1)
    v1, v2 = _make_vehicle(h), _make_vehicle(h)
    t = datetime(2026, 9, 21, 13, 0, tzinfo=timezone.utc)
    client.post("/api/sync/batches", json=_batch(dev, [
        _event(1, v1, station["id"], occurred_at=t),
        _event(2, v2, station["id"], occurred_at=t + timedelta(minutes=1)),
    ]), headers=h)
    conflict = client.get("/api/sync/events",
                          params={"device_id": dev, "status_filter": "conflict"}, headers=h).json()[0]

    # 站点补充电池
    client.put(f"/api/stations/{station['id']}",
               json={"slot_total": 5, "battery_ready": 4}, headers=h)
    resp = client.post(f"/api/sync/events/{conflict['id']}/resolve",
                       json={"action": "apply", "reason": "站点已完成电池补货，核实后补应用"}, headers=h)
    assert resp.status_code == 200, resp.text
    assert resp.json()["resolution_action"] == "applied" and resp.json()["swap_record_id"]
    cur = client.get(f"/api/sync/devices/{dev}", headers=h).json()
    assert cur["confirmed_seq"] == 2
    assert client.get(f"/api/vehicles/{v2}", headers=h).json()["current_soc"] == 100.0


# ---------- 7. 在线交易后，旧离线事件不得倒写 ----------
def test_online_swap_blocks_stale_offline_backwrite():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v = _make_vehicle(h, soc=30.0)
    ready0 = station["battery_ready"]

    # 车辆刚刚通过在线接口完成换电（当前时间），电量 100
    online = client.post("/api/swaps", json={
        "vehicle_id": v, "station_id": station["id"], "soc_before": 30.0, "soc_after": 100.0,
    }, headers=h)
    assert online.status_code == 201

    # 终端重连后带来一笔“发生在 2 小时前”的离线事件，会把电量改回旧值 -> 必须拒绝
    old = _event(1, v, station["id"],
                 occurred_at=datetime.now(timezone.utc) - timedelta(hours=2),
                 soc_before=25.0, soc_after=100.0)
    resp = client.post("/api/sync/batches", json=_batch(dev, [old]), headers=h)
    r = resp.json()["results"][0]
    assert r["status"] == "conflict" and "倒写" in r["reason"]
    # 车辆电量仍是在线交易后的 100（在线结果未被旧值覆盖语义），库存未被该事件扣减
    assert client.get(f"/api/vehicles/{v}", headers=h).json()["current_soc"] == 100.0
    ready = client.get(f"/api/stations/{station['id']}", headers=h).json()["battery_ready"]
    assert ready == ready0 - 1  # 只有在线那一笔扣了库存


# ---------- 8. 会话代次切换：旧代次未决事件转冲突，旧代次重放被拒 ----------
def test_generation_rotation_and_stale_replay():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v1, v2, v3 = _make_vehicle(h), _make_vehicle(h), _make_vehicle(h)

    # 旧代次：seq1 已应用，seq3 到达但缺 seq2（挂起）
    client.post("/api/sync/batches", json=_batch(dev, [
        _event(1, v1, station["id"], occurred_at=datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)),
    ], generation="gen-A"), headers=h)
    ready_mid = client.get(f"/api/stations/{station['id']}", headers=h).json()["battery_ready"]
    client.post("/api/sync/batches", json=_batch(dev, [
        _event(3, v3, station["id"], occurred_at=datetime(2026, 9, 21, 9, 5, tzinfo=timezone.utc)),
    ], generation="gen-A"), headers=h)

    # 设备重置，新代次（起始时间更晚）开始：旧代次 seq3 挂起事件应转冲突
    new_start = datetime(2026, 9, 22, 0, 0, tzinfo=timezone.utc)
    resp = client.post("/api/sync/batches", json=_batch(
        dev, [_event(1, v2, station["id"], occurred_at=datetime(2026, 9, 22, 0, 10, tzinfo=timezone.utc))],
        generation="gen-B", started_at=new_start), headers=h)
    assert resp.status_code == 200, resp.text
    assert resp.json()["confirmed_seq"] == 1
    old_conflicts = client.get("/api/sync/events",
                               params={"device_id": dev, "status_filter": "conflict"}, headers=h).json()
    assert any("新会话代次" in c["conflict_reason"] for c in old_conflicts)

    # 旧代次重放 -> 409
    stale = client.post("/api/sync/batches", json=_batch(
        dev, [_event(2, v1, station["id"])], generation="gen-A"), headers=h)
    assert stale.status_code == 409
    assert stale.json()["detail"]["current_generation"] == "gen-B"

    # 库存只由：旧代次 seq1 + 新代次 seq1 扣减（旧 seq3 未自动应用）
    ready = client.get(f"/api/stations/{station['id']}", headers=h).json()["battery_ready"]
    assert ready == ready_mid - 1


# ---------- 9. 服务重启不丢队列，启动续推 ----------
def test_pending_queue_survives_restart_and_pump_advances():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v1, v2 = _make_vehicle(h), _make_vehicle(h)

    # seq2 先挂起（缺 seq1）
    client.post("/api/sync/batches", json=_batch(dev, [
        _event(2, v2, station["id"], occurred_at=datetime(2026, 9, 21, 15, 2, tzinfo=timezone.utc)),
    ]), headers=h)

    # 模拟“服务重启”：丢弃所有 ORM 会话状态，只靠持久化数据；
    # 直接把 seq1 补进库里（不经过泵），再执行启动续推
    db = SessionLocal()
    try:
        cursor = db.get(DeviceCursor, dev)
        from app.models import OfflineEvent as OE
        import json as _json
        from app.services.sync import canonical_payload
        from app.schemas import OfflineEventIn
        ev_in = OfflineEventIn(**_event(1, v1, station["id"],
                                        occurred_at=datetime(2026, 9, 21, 15, 1, tzinfo=timezone.utc)))
        import hashlib
        canon = canonical_payload(ev_in)
        db.add(OE(device_id=dev, generation="gen-1", seq=1, event_uid=ev_in.event_uid,
                  payload_json=canon, payload_sha256=hashlib.sha256(canon.encode()).hexdigest(),
                  status="waiting"))
        db.commit()
    finally:
        db.close()

    # 启动时的续推动作
    db = SessionLocal()
    try:
        moved = pump_all_devices(db)
    finally:
        db.close()
    assert moved >= 1
    cur = client.get(f"/api/sync/devices/{dev}", headers=h).json()
    assert cur["confirmed_seq"] == 2 and cur["missing_seqs"] == []


# ---------- 10. 同设备并发提交同一批：库存至多扣一次 ----------
def test_concurrent_same_batch_dedup_single_inventory_effect():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v = _make_vehicle(h)
    ev = _event(1, v, station["id"])
    batch = _batch(dev, [ev])

    outcomes = []
    errors = []

    def worker():
        # 每个线程独立的 TestClient（portal 非线程安全），共用同一个 SQLite 文件
        local_client = TestClient(app)
        try:
            r = local_client.post("/api/sync/batches", json=batch, headers=h)
            outcomes.append(r.json())
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors

    applied = sum(o["applied_count"] for o in outcomes)
    duplicates = sum(o["duplicate_count"] for o in outcomes)
    assert applied == 1 and duplicates == 3
    ready = client.get(f"/api/stations/{station['id']}", headers=h).json()["battery_ready"]
    assert ready == station["battery_ready"] - 1  # 恰好扣一次


# ---------- 11. 对账：每个生效事件恰好一条换电记录，无倒写 ----------
def test_reconcile_proves_exactly_once():
    resp = client.get("/api/sync/reconcile", headers=_auth())
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True, body
    assert body["effective_event_count"] == body["offline_swap_record_count"]
    assert body["duplicate_application_events"] == []
    assert body["backwrite_risks"] == []
    assert body["applied_without_swap"] == []
    assert body["swap_without_event"] == []


# ---------- 12. 设备未注册时查询游标返回 404 ----------
def test_unknown_device_cursor_404():
    resp = client.get(f"/api/sync/devices/nonexistent-{uuid.uuid4().hex}", headers=_auth())
    assert resp.status_code == 404


# ---------- 13. 单批内乱序到达，同一事务内连续全部应用 ----------
def test_single_batch_arrives_shuffled():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v1, v2, v3 = _make_vehicle(h), _make_vehicle(h), _make_vehicle(h)
    t = datetime(2026, 9, 21, 16, 0, tzinfo=timezone.utc)
    resp = client.post("/api/sync/batches", json=_batch(dev, [
        _event(3, v3, station["id"], occurred_at=t + timedelta(minutes=3)),
        _event(1, v1, station["id"], occurred_at=t + timedelta(minutes=1)),
        _event(2, v2, station["id"], occurred_at=t + timedelta(minutes=2)),
    ]), headers=h)
    body = resp.json()
    assert body["confirmed_seq"] == 3 and body["applied_count"] == 3
    assert {r["status"] for r in body["results"]} == {"applied"}
    ready = client.get(f"/api/stations/{station['id']}", headers=h).json()["battery_ready"]
    assert ready == station["battery_ready"] - 3


# ---------- 14. 中间事件冲突会阻断其后事件，解决后自动续推 ----------
def test_conflict_blocks_subsequent_until_resolved():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h, count=1)
    v1, v2, v3 = _make_vehicle(h), _make_vehicle(h), _make_vehicle(h)
    t = datetime(2026, 9, 21, 17, 0, tzinfo=timezone.utc)
    # seq1 用掉最后一块电池；seq2 冲突；seq3 序号连续但必须被 seq2 挡住
    client.post("/api/sync/batches", json=_batch(dev, [
        _event(1, v1, station["id"], occurred_at=t),
        _event(2, v2, station["id"], occurred_at=t + timedelta(minutes=1)),
        _event(3, v3, station["id"], occurred_at=t + timedelta(minutes=2)),
    ]), headers=h)
    cur = client.get(f"/api/sync/devices/{dev}", headers=h).json()
    assert cur["confirmed_seq"] == 1  # seq2 冲突，seq3 不得越过
    events = {e["seq"]: e for e in client.get(
        "/api/sync/events", params={"device_id": dev}, headers=h).json()}
    assert events[2]["status"] == "conflict"
    assert events[3]["status"] == "waiting"

    # 补货后人工应用 seq2，seq3 应被闸门自动续推（此时库存足够）
    client.put(f"/api/stations/{station['id']}", json={"slot_total": 5, "battery_ready": 4}, headers=h)
    ok = client.post(f"/api/sync/events/{events[2]['id']}/resolve",
                     json={"action": "apply", "reason": "补货核实，补应用"}, headers=h)
    assert ok.status_code == 200
    cur = client.get(f"/api/sync/devices/{dev}", headers=h).json()
    assert cur["confirmed_seq"] == 3
    events = {e["seq"]: e for e in client.get(
        "/api/sync/events", params={"device_id": dev}, headers=h).json()}
    assert events[3]["status"] == "applied"


# ---------- 15. 同批内重复事件标识/序号被快速拒绝 ----------
def test_duplicate_within_batch_rejected():
    h = _auth()
    dev = f"term-{uuid.uuid4().hex[:8]}"
    station = _station_with_batteries(h)
    v = _make_vehicle(h)
    ev = _event(1, v, station["id"])
    resp = client.post("/api/sync/batches", json=_batch(dev, [ev, dict(ev, seq=2)]), headers=h)
    assert resp.status_code == 422 and "事件标识" in resp.json()["detail"]
    ev2 = _event(2, v, station["id"], uid="uid-X")
    resp2 = client.post("/api/sync/batches", json=_batch(dev, [
        {**ev2, "seq": 1, "event_uid": "uid-X"},
        {**ev2, "seq": 1, "event_uid": "uid-Y"},
    ]), headers=h)
    assert resp2.status_code == 422 and "重复序号" in resp2.json()["detail"]
