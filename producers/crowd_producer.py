import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER = 'localhost:9092' 
TOPIC = 'crowd-reports'

# Granice Niša (za generisanje prijava unutar grada)
NS_LAT = (43.310, 43.355)
NS_LON = (21.880, 21.930)

# Povremeno se generise prijava van granica grada - sum
NOISE_LAT = (43.295, 43.370)
NOISE_LON = (21.860, 21.945)

def in_city(lat, lon):
    return (NS_LAT[0] <= lat <= NS_LAT[1] and
            NS_LON[0] <= lon <= NS_LON[1])

def make_report(lat, lon):
    return {
        "report_id": f"R-{random.randint(10000, 99999)}",
        "lat":       round(lat, 6),
        "lon":       round(lon, 6),
        "timestamp": datetime.utcnow().isoformat(),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    print("[Producer] Pokrenut.")

    # 4-7 aktivnih tacaka oko kojih se grupisu prijave
    hotspots = [
        (round(random.uniform(*NS_LAT), 6),
         round(random.uniform(*NS_LON), 6))
        for _ in range(random.randint(4, 7))
    ]

    cycle = 0
    while True:
        cycle += 1

        # Svakih 6 ciklusa - promeni jedan hotspot
        if cycle % 6 == 0:
            idx = random.randrange(len(hotspots))
            hotspots[idx] = (
                round(random.uniform(*NS_LAT), 6),
                round(random.uniform(*NS_LON), 6)
            )

        # Izaberemo 1 do 3 aktivna hotspota za ovaj ciklus
        num_active = min(len(hotspots), random.randint(1, 3))
        active_hotspots = random.sample(hotspots, num_active)

        for hs_lat, hs_lon in active_hotspots: # Za svaki izabrani hotspot
            # Šaljemo samo 1 do 2 prijave po izabranom hotspotu
            for _ in range(random.randint(1, 2)):
                lat = round(hs_lat + random.uniform(-0.0006, 0.0006), 6)
                lon = round(hs_lon + random.uniform(-0.0006, 0.0006), 6)
                producer.send(TOPIC, value=make_report(lat, lon))

        # Povremeno generiši šum van granica
        if random.random() < 0.2: # 20% šanse da se pojavi šum
            lat = round(random.uniform(*NOISE_LAT), 6)
            lon = round(random.uniform(*NOISE_LON), 6)
            while in_city(lat, lon):
                lat = round(random.uniform(*NOISE_LAT), 6)
                lon = round(random.uniform(*NOISE_LON), 6)
            producer.send(TOPIC, value=make_report(lat, lon))

        producer.flush()
        time.sleep(12) # Pauza od 12 sekundi između ciklusa (prilagoditi po potrebi)

if __name__ == "__main__":
    main()