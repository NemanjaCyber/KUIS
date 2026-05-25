import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

import config

KAFKA_BROKER = config.KAFKA_BROKER
TOPIC = config.KAFKA_INPUT_TOPIC

NS_LAT = (float(config.NS_LAT_MIN), float(config.NS_LAT_MAX))
NS_LON = (float(config.NS_LON_MIN), float(config.NS_LON_MAX))

NOISE_LAT = (float(config.NOISE_LAT_MIN), float(config.NOISE_LAT_MAX))
NOISE_LON = (float(config.NOISE_LON_MIN), float(config.NOISE_LON_MAX))

PRODUCER_SLEEP_SEC = float(config.PRODUCER_SLEEP_SEC)

def in_city(lat, lon):# Provera da li su koordinate unutar granica grada
    return (NS_LAT[0] <= lat <= NS_LAT[1] and
            NS_LON[0] <= lon <= NS_LON[1])

def make_report(lat, lon):# Generisanje prijave sa jedinstvenim ID-jem, koordinatama i vremenom
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

    # 4-7 aktivnih tacaka (hotspotova) oko kojih se grupisu prijave
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

        # Izaberemo 1 do 3 aktivna hotspota za trenutni ciklus
        num_active = min(len(hotspots), random.randint(1, 3))
        active_hotspots = random.sample(hotspots, num_active)

        for hs_lat, hs_lon in active_hotspots: # Za svaki izabrani hotspot
            # Šaljemo samo 1 do 2 prijave po izabranom hotspotu
            for _ in range(random.randint(1, 2)):
                lat = round(hs_lat + random.uniform(-0.0006, 0.0006), 6)
                lon = round(hs_lon + random.uniform(-0.0006, 0.0006), 6)
                producer.send(TOPIC, value=make_report(lat, lon))

        # Povremeno generiši šum van granica grada
        if random.random() < 0.2: # 20% šanse da se pojavi šum
            lat = round(random.uniform(*NOISE_LAT), 6)
            lon = round(random.uniform(*NOISE_LON), 6)
            while in_city(lat, lon):
                lat = round(random.uniform(*NOISE_LAT), 6)
                lon = round(random.uniform(*NOISE_LON), 6)
            producer.send(TOPIC, value=make_report(lat, lon))

        producer.flush()
        time.sleep(PRODUCER_SLEEP_SEC)# Vreme između generisanja novih prijava od strane producer-a

if __name__ == "__main__":
    main()