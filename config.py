# ── KAFKA KONFIGURACIJA ──────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092" 
KAFKA_INPUT_TOPIC = "crowd-reports"# Ulazni topic za prijave građana
KAFKA_INCIDENT_TOPIC = "verified-incidents"# Topic na koji se šalju verifikovani incidenti

# ── PARAMETRI KLASTERIZACIJE I VERIFIKACIJE ──────────────────────────────────
CLUSTER_RADIUS_M = 120# Radijus klastera u metrima
INCIDENT_THRESHOLD = 5# Minimalan broj prijava da bi se formirao incident
CLUSTER_TTL_SEC = 90# Vreme trajanja klastera pre nego što se obriše (ako nije verifikovan)

# ── GEOGRAFSKE GRANICE NIŠA ──────────────────────────────────────────────────
NS_LAT_MIN = 43.310
NS_LAT_MAX = 43.355
NS_LON_MIN = 21.880
NS_LON_MAX = 21.930

# ── GRANICE ZA GENERISANJE ŠUMA (PRODUCER) ───────────────────────────────────
NOISE_LAT_MIN = 43.295
NOISE_LAT_MAX = 43.370
NOISE_LON_MIN = 21.860
NOISE_LON_MAX = 21.945

# ── TAJMING I SIMULACIJA ──────────────────────────────────────────────────────
PRODUCER_SLEEP_SEC = 12# Vreme između generisanja novih prijava od strane producer-a
SCENE_WAIT_SEC = 5# Vreme čekanja vozila na mestu incidenta pre nego što krenu ka sledećem zadatku
VEHICLE_MOVE_INTERVAL = 0.8# Vreme između pomeranja vozila u simulaciji (manje = brže pomeranje)
BROADCAST_INTERVAL = 1.2# Vreme između emitovanja stanja vozila i incidenata svim klijentima preko WebSocket-a

# ── EKSTERNI SERVISI ──────────────────────────────────────────────────────────
OSRM_URL = "http://router.project-osrm.org/route/v1/driving/"