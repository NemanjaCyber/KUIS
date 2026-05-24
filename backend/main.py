import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json, asyncio, threading, requests, math
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from dotenv import load_dotenv

import config

from consumer.processor import situation, lock, start as start_processor, resolve_incident

app = FastAPI()
threading.Thread(target=start_processor, daemon=True).start()
connected_clients = []

OSRM_URL = config.OSRM_URL
SCENE_WAIT_SEC = config.SCENE_WAIT_SEC
VEHICLE_MOVE_INTERVAL = config.VEHICLE_MOVE_INTERVAL
BROADCAST_INTERVAL = config.BROADCAST_INTERVAL

# ── OSRM ─────────────────────────────────────────────────────────────────────
def get_route(slat, slon, elat, elon):
    url = (f"{OSRM_URL}"
           f"{slon},{slat};{elon},{elat}"
           f"?overview=full&geometries=geojson")
    try:
        r = requests.get(url, timeout=5)
        d = r.json()
        if d["code"] == "Ok":
            return [[c[1], c[0]] for c in d["routes"][0]["geometry"]["coordinates"]]
    except Exception as e:
        print(f"[OSRM] {e}")
    return []

def haversine(lat1, lon1, lat2, lon2):
    R, p = 6371000, math.pi / 180
    a = (math.sin((lat2-lat1)*p/2)**2 +
         math.cos(lat1*p)*math.cos(lat2*p)*math.sin((lon2-lon1)*p/2)**2)
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

# ── Vozila ────────────────────────────────────────────────────────────────────
vehicles = { # vozila u STANDBY poziciji, spremna za kretanje
    "V-101": {"id":"V-101","name":"Vozilo 1","lat":43.3328,"lon":21.9050,
               "status":"STANDBY","mission":None,"route":[],"step":0},
    "V-102": {"id":"V-102","name":"Vozilo 2","lat":43.3250,"lon":21.8950,
               "status":"STANDBY","mission":None,"route":[],"step":0},
}

def get_active_incidents():
    with lock:
        return {
            k: v for k, v in situation["incidents"].items()
            if v["status"] == "ACTIVE"
        }

def assign():
    """Dodeli slobodna vozila nepokriverim incidentima.
       Prioritet = veci broj prijava"""
    active = get_active_incidents()
    if not active:
        return

    covered = {v["mission"] for v in vehicles.values() if v["mission"]}

    # Sortiraj po broju prijava - vise prijava = veci prioritet
    uncovered = sorted(
        [i for i in active.values() if i["id"] not in covered],
        key=lambda i: i["report_count"],
        reverse=True
    )

    for inc in uncovered:
        free = [v for v in vehicles.values() if v["status"] == "STANDBY"]
        if not free:
            break
        # Najbliže slobodno vozilo
        v = min(free, key=lambda v: haversine(v["lat"],v["lon"],inc["lat"],inc["lon"]))
        route = get_route(v["lat"], v["lon"], inc["lat"], inc["lon"])
        v.update({"status":"EN_ROUTE","mission":inc["id"],"route":route,"step":0})
        print(f"[Dispatch] {v['id']} -> {inc['id']} ({inc['report_count']} prijava)")

async def move_vehicles():# 
    while True:
        await asyncio.sleep(VEHICLE_MOVE_INTERVAL) # Brzina pomeranja definisana u .env

        for v in vehicles.values():

            if v["status"] == "EN_ROUTE":
                route, step = v["route"], v["step"]
                if not route:
                    v["status"] = "STANDBY"
                    v["mission"] = None
                    continue

                # Pomeri 2 koraka po tick-u 
                for _ in range(2):
                    if v["step"] < len(route):
                        v["lat"] = route[v["step"]][0]
                        v["lon"] = route[v["step"]][1]
                        v["step"] += 1
                    else:
                        # Stiglo - predje u ON_SCENE
                        v["status"]     = "ON_SCENE"
                        v["route"]      = []
                        v["step"]       = 0
                        v["scene_until"] = asyncio.get_event_loop().time() + SCENE_WAIT_SEC
                        print(f"[Vehicle] {v['id']} na mestu incidenta {v['mission']}, ceka {SCENE_WAIT_SEC}s")
                        break

            elif v["status"] == "ON_SCENE":
                now = asyncio.get_event_loop().time()
                if now >= v.get("scene_until", 0):
                    # Resi incident
                    iid = v["mission"]
                    resolve_incident(iid, v["name"])
                    v.update({"status":"STANDBY","mission":None,"route":[],"step":0})
                    print(f"[Vehicle] {v['id']} zavrsio, prelazi u STANDBY")

        assign()

        # Sync u situation za broadcast
        with lock:
            situation["vehicles"] = [
                {
                    "id":     v["id"],
                    "name":   v["name"],
                    "lat":    v["lat"],
                    "lon":    v["lon"],
                    "status": v["status"],
                    "mission":v["mission"],
                }
                for v in vehicles.values()
            ]

# ── HTTP / WS ─────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    with open("frontend/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            await asyncio.wait_for(websocket.receive_text(), timeout=30)
    except (WebSocketDisconnect, asyncio.TimeoutError):
        if websocket in connected_clients:
            connected_clients.remove(websocket)

async def broadcast():
    while True:
        await asyncio.sleep(BROADCAST_INTERVAL) # Interval slanja snapshot-a
        if not connected_clients:
            continue
        with lock:
            payload = json.dumps({
                "type":      "update",
                "clusters":  list(situation["clusters"].values()),
                "incidents": list(situation["incidents"].values()),
                "vehicles":  situation.get("vehicles", []),
                "all_reports": situation.get("all_reports", []),
                "resolved":  situation["resolved"],
                "noise":     situation["noise_reports"],
                "stats":     situation["stats"],
            })
        dead = []
        for ws in connected_clients:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            connected_clients.remove(ws)

@app.on_event("startup")
async def startup():# Pokreni broadcast i simulaciju vozila paralelno
    asyncio.create_task(broadcast())
    asyncio.create_task(move_vehicles())