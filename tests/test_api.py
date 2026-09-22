"""接口冒烟测试：覆盖认证、鉴权、CRUD、换电与统计，并校验中文编码。"""
import uuid

from fastapi.testclient import TestClient

from app.main import app
from app.seed import init_db

init_db()
client = TestClient(app)


def _login() -> str:
    resp = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def _auth_headers() -> dict:
    return {"Authorization": f"Bearer {_login()}"}


def test_health():
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_login_wrong_password():
    resp = client.post("/api/auth/login", json={"username": "admin", "password": "bad"})
    assert resp.status_code == 401


def test_requires_auth():
    # 未带 token 访问受保护资源应被拦截
    resp = client.get("/api/stations")
    assert resp.status_code == 401


def test_me_and_chinese_encoding():
    resp = client.get("/api/auth/me", headers=_auth_headers())
    assert resp.status_code == 200
    # 中文显示名必须正确返回，验证 UTF-8 编码无乱码
    assert resp.json()["display_name"] == "平台管理员"


def test_seed_stations_present_with_chinese():
    resp = client.get("/api/stations", headers=_auth_headers())
    assert resp.status_code == 200
    stations = resp.json()
    assert len(stations) >= 4
    assert any("换电站" in s["name"] for s in stations)


def test_station_crud_and_validation():
    headers = _auth_headers()
    # 非法数据：满电电池数 > 仓位总数
    bad = client.post("/api/stations", json={"name": "测试站", "slot_total": 2, "battery_ready": 5}, headers=headers)
    assert bad.status_code == 422

    created = client.post(
        "/api/stations",
        json={"name": "西站测试换电站", "address": "测试路 1 号", "slot_total": 10, "battery_ready": 6},
        headers=headers,
    )
    assert created.status_code == 201, created.text
    sid = created.json()["id"]
    assert created.json()["name"] == "西站测试换电站"

    updated = client.put(f"/api/stations/{sid}", json={"status": "maintenance"}, headers=headers)
    assert updated.status_code == 200
    assert updated.json()["status"] == "maintenance"

    deleted = client.delete(f"/api/stations/{sid}", headers=headers)
    assert deleted.status_code == 204
    assert client.get(f"/api/stations/{sid}", headers=headers).status_code == 404


def test_vehicle_unique_plate():
    headers = _auth_headers()
    plate = f"测{uuid.uuid4().hex[:6]}"
    first = client.post("/api/vehicles", json={"plate": plate, "model": "测试车型"}, headers=headers)
    assert first.status_code == 201, first.text
    dup = client.post("/api/vehicles", json={"plate": plate}, headers=headers)
    assert dup.status_code == 409


def test_swap_flow_updates_state():
    headers = _auth_headers()
    # 取一个有满电电池的运营站
    stations = client.get("/api/stations", headers=headers).json()
    station = next(s for s in stations if s["battery_ready"] > 0)
    plate = f"沪EV{uuid.uuid4().hex[:4]}"
    vehicle = client.post(
        "/api/vehicles", json={"plate": plate, "model": "换电测试车", "current_soc": 10.0}, headers=headers
    ).json()

    before_ready = station["battery_ready"]
    swap = client.post(
        "/api/swaps",
        json={"vehicle_id": vehicle["id"], "station_id": station["id"], "soc_before": 10.0, "soc_after": 100.0},
        headers=headers,
    )
    assert swap.status_code == 201, swap.text
    assert swap.json()["station_name"] == station["name"]

    # 车辆电量应更新、站点可用电池应减一
    v_after = client.get(f"/api/vehicles/{vehicle['id']}", headers=headers).json()
    assert v_after["current_soc"] == 100.0
    s_after = client.get(f"/api/stations/{station['id']}", headers=headers).json()
    assert s_after["battery_ready"] == before_ready - 1


def test_swap_invalid_soc():
    headers = _auth_headers()
    stations = client.get("/api/stations", headers=headers).json()
    station = next(s for s in stations if s["battery_ready"] > 0)
    vehicles = client.get("/api/vehicles", headers=headers).json()
    bad = client.post(
        "/api/swaps",
        json={"vehicle_id": vehicles[0]["id"], "station_id": station["id"], "soc_before": 90.0, "soc_after": 50.0},
        headers=headers,
    )
    assert bad.status_code == 422


def test_dashboard_stats():
    resp = client.get("/api/dashboard/stats", headers=_auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert data["station_total"] >= 4
    assert data["vehicle_total"] >= 5
    assert "battery_ready_total" in data
