import json
import math
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone

KAFKA_BROKER       = 'localhost:9092'
INPUT_TOPIC        = 'crowd-reports'
INCIDENT_TOPIC     = 'verified-incidents'

CLUSTER_RADIUS_M   = 120   # prijave unutar 120m idu u isti klaster
INCIDENT_THRESHOLD = 5     # klaster postaje incident na 5+ prijava
CLUSTER_TTL_SEC    = 90    # klaster se brise ako nema novih prijava

# Granice Novog Sada
NS_LAT = (45.230, 45.285)
NS_LON = (19.790, 19.890)

# ── Shared state ──────────────────────────────────────────────────────────────
_clusters  = {}   # interno - puni objekti sa listom prijava
_incidents = {}   # interno

situation = {
    "clusters":          {},   # snapshot za frontend (bez liste prijava)
    "incidents":         {},
    "resolved":          [],   # lista resenih incidenata
    "noise_reports":     [],   # suma prijave
    "stats": {
        "total":    0,
        "noise":    0,
        "clusters": 0,
        "incidents":0,
    }
}

lock = threading.Lock()

# ── Geo ───────────────────────────────────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R, p = 6371000, math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2 +
         math.cos(lat1 * p) * math.cos(lat2 * p) *
         math.sin((lon2 - lon1) * p / 2) ** 2)
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def centroid(reports):
    lat = sum(r["lat"] for r in reports) / len(reports)
    lon = sum(r["lon"] for r in reports) / len(reports)
    return round(lat, 6), round(lon, 6)

def in_city(lat, lon):
    return (NS_LAT[0] <= lat <= NS_LAT[1] and
            NS_LON[0] <= lon <= NS_LON[1])

# ── Klasterizacija ────────────────────────────────────────────────────────────
def find_cluster(lat, lon):
    """Vraca ID najblizeg klastera unutar radijusa, ili None."""
    best, best_dist = None, float('inf')
    for cid, cl in _clusters.items():
        d = haversine(lat, lon, cl["lat"], cl["lon"])
        if d <= CLUSTER_RADIUS_M and d < best_dist:
            best, best_dist = cid, d
    return best

def add_to_cluster(cid, report):
    cl = _clusters[cid]
    cl["reports"].append(report)
    cl["last_update"] = datetime.utcnow().isoformat()
    # Azuriraj centar
    lat, lon = centroid(cl["reports"])
    cl["lat"] = lat
    cl["lon"] = lon
    return cl

def new_cluster(report):
    cid = f"CL-{situation['stats']['clusters'] + 1:03d}"
    situation["stats"]["clusters"] += 1
    _clusters[cid] = {
        "id":          cid,
        "lat":         report["lat"],
        "lon":         report["lon"],
        "reports":     [report],
        "verified":    False,
        "last_update": datetime.utcnow().isoformat(),
        "created_at":  datetime.utcnow().isoformat(),
    }
    return cid

# ── Validacija ────────────────────────────────────────────────────────────────
def try_verify(cid, producer):
    cl = _clusters[cid]
    count = len(cl["reports"])

    if count < INCIDENT_THRESHOLD:
        return

    if cl["verified"]:
        # Vec verifikovan - samo azuriraj broj prijava u incidentu
        if cid in _incidents:
            _incidents[cid]["report_count"] = count
            _incidents[cid]["lat"] = cl["lat"]
            _incidents[cid]["lon"] = cl["lon"]
        return

    cl["verified"] = True
    situation["stats"]["incidents"] += 1

    incident = {
        "id":           cid,
        "lat":          cl["lat"],
        "lon":          cl["lon"],
        "report_count": count,
        "status":       "ACTIVE",
        "verified_at":  datetime.utcnow().isoformat(),
        "resolved_at":  None,
        "resolved_by":  None,
    }
    _incidents[cid] = incident
    producer.send(INCIDENT_TOPIC, value=incident)
    producer.flush()
    print(f"  [+] Incident {cid} verifikovan ({count} prijava)")

# ── Reset (poziva backend kada vozilo resi incident) ──────────────────────────
def resolve_incident(incident_id, vehicle_name):
    """Backend poziva ovu funkciju kada vozilo stigne i resi incident."""
    with lock:
        inc = _incidents.get(incident_id)
        if not inc:
            return

        inc["status"]      = "RESOLVED"
        inc["resolved_at"] = datetime.utcnow().isoformat()
        inc["resolved_by"] = vehicle_name

        # Dodaj u listu resenih
        situation["resolved"].insert(0, {
            "id":          inc["id"],
            "resolved_at": inc["resolved_at"],
            "resolved_by": vehicle_name,
            "report_count": inc["report_count"],
        })
        # Max 20 resenih u listi
        situation["resolved"] = situation["resolved"][:20]

        # Ocisti klaster i incident iz aktivnog state-a
        _clusters.pop(incident_id, None)
        _incidents.pop(incident_id, None)
        situation["clusters"].pop(incident_id, None)
        situation["incidents"].pop(incident_id, None)

        print(f"  [✓] Incident {incident_id} resen od strane {vehicle_name}")

# ── TTL cleanup ───────────────────────────────────────────────────────────────
def cleanup_loop():
    while True:
        time.sleep(20)
        now = datetime.now(timezone.utc)
        with lock:
            stale = []
            for cid, cl in _clusters.items():
                last = datetime.fromisoformat(
                    cl["last_update"].replace("Z", "")
                ).replace(tzinfo=timezone.utc)
                age = (now - last).total_seconds()
                if age > CLUSTER_TTL_SEC:
                    stale.append(cid)

            for cid in stale:
                cl = _clusters[cid]
                n  = len(cl["reports"])
                print(f"  [~] Klaster {cid} istekao ({n} prijava, TTL)")
                if n < INCIDENT_THRESHOLD:
                    situation["stats"]["noise"] += n
                    # Dodaj u noise listu
                    situation["noise_reports"].insert(0, {
                        "cluster_id":  cid,
                        "report_count": n,
                        "lat":         cl["lat"],
                        "lon":         cl["lon"],
                        "reason":      f"Nedovoljno prijava ({n} od {INCIDENT_THRESHOLD})",
                        "expired_at":  datetime.utcnow().isoformat(),
                    })
                    situation["noise_reports"] = situation["noise_reports"][:20]

                del _clusters[cid]
                situation["clusters"].pop(cid, None)
                if cid in _incidents:
                    del _incidents[cid]
                    situation["incidents"].pop(cid, None)

# ── Glavni consumer ───────────────────────────────────────────────────────────
def consume_reports():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='kis-processor-v2',
        api_version=(0, 10, 1)
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )

    print("[Processor] Aktivan.")

    for msg in consumer:
        r = msg.value
        situation["stats"]["total"] += 1

        lat = r.get("lat")
        lon = r.get("lon")

        if lat is None or lon is None:
            continue

        # Filtriranje suma - van granica grada
        if not in_city(lat, lon):
            situation["stats"]["noise"] += 1
            situation["noise_reports"].insert(0, {
                "report_id":   r.get("report_id", "?"),
                "lat":         lat,
                "lon":         lon,
                "reason":      "Van granica grada",
                "received_at": r.get("timestamp", datetime.utcnow().isoformat()),
            })
            situation["noise_reports"] = situation["noise_reports"][:20]
            continue

        with lock:
            cid = find_cluster(lat, lon)

            if cid:
                add_to_cluster(cid, r)
                tag = "postojeci"
            else:
                cid = new_cluster(r)
                tag = "novi"

            cl = _clusters[cid]
            count = len(cl["reports"])
            print(f"  [>] ({lat}, {lon}) -> {cid} ({tag}) | {count} prijava")

            # Azuriraj snapshot
            situation["clusters"][cid] = {
                "id":           cid,
                "lat":          cl["lat"],
                "lon":          cl["lon"],
                "report_count": count,
                "verified":     cl["verified"],
                "last_update":  cl["last_update"],
            }
            situation["incidents"] = {
                k: dict(v) for k, v in _incidents.items()
            }

            try_verify(cid, producer)

def start():
    threading.Thread(target=cleanup_loop, daemon=True).start()
    consume_reports()

if __name__ == "__main__":
    start()