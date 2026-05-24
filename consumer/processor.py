import json
import math
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

import config

KAFKA_BROKER       = config.KAFKA_BROKER
INPUT_TOPIC        = config.KAFKA_INPUT_TOPIC# Ulazni topic za prijave građana
INCIDENT_TOPIC     = config.KAFKA_INCIDENT_TOPIC# Topic na koji se šalju verifikovani incidenti

CLUSTER_RADIUS_M   = config.CLUSTER_RADIUS_M# Radijus klastera u metrima
INCIDENT_THRESHOLD = config.INCIDENT_THRESHOLD# Minimalan broj prijava da bi se formirao incident
CLUSTER_TTL_SEC    = config.CLUSTER_TTL_SEC# Vreme trajanja klastera pre nego što se obriše (ako nije verifikovan)

NS_LAT = (float(config.NS_LAT_MIN), float(config.NS_LAT_MAX))
NS_LON = (float(config.NS_LON_MIN), float(config.NS_LON_MAX))

# ── Shared state ──────────────────────────────────────────────────────────────
_clusters  = {}   # interno - puni objekti sa listom prijava
_incidents = {}   # interno

situation = {
    "clusters":          {},   # snapshot za frontend (bez liste prijava)
    "incidents":         {},
    "all_reports":       [],   # sve prijave za prikaz na mapi
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
def haversine(lat1, lon1, lat2, lon2):# Funkcija za izračunavanje udaljenosti između dve geografske tačke koristeći Haversine formulu. Vraća udaljenost u metrima.
    R, p = 6371000, math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2 +
         math.cos(lat1 * p) * math.cos(lat2 * p) *
         math.sin((lon2 - lon1) * p / 2) ** 2)
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def centroid(reports):# Funkcija za izračunavanje geometrijskog centra (centroida) skupa prijava. Vraća latitudu i longitudu centra, zaokružene na 6 decimala.
    lat = sum(r["lat"] for r in reports) / len(reports)
    lon = sum(r["lon"] for r in reports) / len(reports)
    return round(lat, 6), round(lon, 6)

def in_city(lat, lon):# 
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
    # KORISTIMO BROJ KLASTERA ZA ID (on je uvek jedinstven i raste)
    next_cluster_num = situation['stats']['clusters'] + 1
    cid = f"INC-{next_cluster_num:03d}"
    
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
        situation["resolved"] = situation["resolved"][:20]

        # Ukloni sve pojedinačne zelene tačkice vezane za ovaj incident
        situation["all_reports"] = [r for r in situation["all_reports"] if r.get("cluster_id") != incident_id]

        # Ocisti klaster i incident iz aktivnog state-a
        _clusters.pop(incident_id, None)
        _incidents.pop(incident_id, None)
        situation["clusters"].pop(incident_id, None)
        situation["incidents"].pop(incident_id, None)

        print(f"  [✓] Incident {incident_id} resen, očišćene pripadajuće prijave.")

# ── TTL cleanup ───────────────────────────────────────────────────────────────
def cleanup_loop():# Ova funkcija se pokreće u posebnom threadu i periodično proverava da li neki klasteri nisu ažurirani duže od CLUSTER_TTL_SEC. 
    #Ako pronađe takve klastere, briše ih i klasifikuje kao šum ako nisu dostigli threshold za incident.
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
                    situation["noise_reports"].insert(0, {
                        "cluster_id":  cid,
                        "report_count": n,
                        "lat":         cl["lat"],
                        "lon":         cl["lon"],
                        "reason":      f"Nedovoljno prijava ({n} od {INCIDENT_THRESHOLD})",
                        "expired_at":  datetime.utcnow().isoformat(),
                    })
                    situation["noise_reports"] = situation["noise_reports"][:20]

                # Ako klaster istekne, brišemo i njegove zelene tačkice sa mape
                situation["all_reports"] = [r for r in situation["all_reports"] if r.get("cluster_id") != cid]

                del _clusters[cid]
                situation["clusters"].pop(cid, None)
                if cid in _incidents:
                    del _incidents[cid]
                    situation["incidents"].pop(cid, None)

# ── Glavni consumer ───────────────────────────────────────────────────────────
def consume_reports():#
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

        # Provera da li je prijava van granica grada - ako jeste, klasifikujemo kao šum i ne obrađujemo dalje
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
            # 1. PRVO pronalazimo ili pravimo klaster da bismo definisali 'cid'
            cid = find_cluster(lat, lon)

            if cid:
                add_to_cluster(cid, r)
                tag = "postojeci"
            else:
                cid = new_cluster(r)
                tag = "novi"

            # 2. TEK SADA, kada imamo 'cid', bezbedno dodajemo prijavu u all_reports
            situation["all_reports"].insert(0, {
                "report_id": r.get("report_id"),
                "lat": lat,
                "lon": lon,
                "cluster_id": cid,   # Sada 'cid' sigurno postoji i ispravan je!
                "timestamp": r.get("timestamp", datetime.utcnow().isoformat()),
            })
            
            # limit od 100 prijava na mapi (ukloniti ako zelimo da vidimo sve)
            situation["all_reports"] = situation["all_reports"][:100]

            cl = _clusters[cid]
            count = len(cl["reports"])
            print(f"  [>] ({lat}, {lon}) -> {cid} ({tag}) | {count} prijava")

            # Azuriraj snapshot za frontend
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

            # Pokušaj verifikaciju incidenta
            try_verify(cid, producer)

def start():
    threading.Thread(target=cleanup_loop, daemon=True).start()
    consume_reports()

if __name__ == "__main__":
    start()