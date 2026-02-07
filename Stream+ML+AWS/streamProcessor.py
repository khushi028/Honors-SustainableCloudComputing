import pickle
import json
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import math
import time

# CONFIG
BROKER = "localhost:9092"
INPUT_TOPIC = "sensor-stream"
OUTPUT_TOPIC = "scheduler-topic"

# temperature thresholds
CPU_HIGH = 80
GPU_HIGH = 75
CPU_LOW = 50
GPU_LOW = 45

# stability controls
HIGH_COUNT_THRESHOLD = 3      # need 3 consecutive highs to scale up
LOW_COUNT_THRESHOLD = 5       # need 5 consecutive lows to scale down
COOLDOWN = 300                # seconds between scaling actions


# GLOBAL STATE (important)
high_counter = 0
low_counter = 0
last_action_time = 0
last_sent_action = "NO_ACTION"


# UTILS
def safe_float(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return 0.0
    return float(x)


def allow_action():
    """cooldown guard"""
    global last_action_time
    now = time.time()

    if now - last_action_time > COOLDOWN:
        last_action_time = now
        return True
    return False


# TADR DECISION LOGIC (with hysteresis)
def tadr_decision(cpu_temp, gpu_temp):
    global high_counter, low_counter

    # HIGH condition
    if cpu_temp > CPU_HIGH or gpu_temp > GPU_HIGH:
        high_counter += 1
        low_counter = 0

    # LOW condition
    elif cpu_temp < CPU_LOW and gpu_temp < GPU_LOW:
        low_counter += 1
        high_counter = 0

    # NORMAL
    else:
        high_counter = 0
        low_counter = 0

    # scaling decisions
    if high_counter >= HIGH_COUNT_THRESHOLD:
        high_counter = 0
        return "DISTRIBUTE_LOAD"

    if low_counter >= LOW_COUNT_THRESHOLD:
        low_counter = 0
        return "UNDERUTILIZED"

    return "NORMAL"


ACTION_MAP = {
    "DISTRIBUTE_LOAD": "SCALE_UP",
    "UNDERUTILIZED": "SCALE_DOWN",
    "NORMAL": "NO_ACTION"
}


# LOAD MODELS
print("Loading models...")

with open("models/cpu_temp_30min_model.pkl", "rb") as f:
    cpu_bundle = pickle.load(f)
    cpu_model = cpu_bundle["model"]
    feature_names = cpu_bundle["feature_columns"]

with open("models/gpu_temp_30min_model.pkl", "rb") as f:
    gpu_bundle = pickle.load(f)
    gpu_model = gpu_bundle["model"]

print("✅ Models loaded")
print("Expected feature count:", len(feature_names))


# KAFKA SETUP

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10,          # batching improves performance
    batch_size=16384
)

print("Stream Processor Started...")


# MAIN LOOP
for msg in consumer:

    try:
        data = msg.value

        # Feature vector
        row = [safe_float(data.get(fname, 0.0)) for fname in feature_names]
        features = np.array([row])

        # Predict temps
        # cpu_temp = float(cpu_model.predict(features)[0])
        # gpu_temp = float(gpu_model.predict(features)[0])

        cpu_temp = 90
        gpu_temp = 77

        # Decision
        tadr_action = tadr_decision(cpu_temp, gpu_temp)
        aws_action = ACTION_MAP.get(tadr_action, "NO_ACTION")

        print(
            f"CPU={cpu_temp:.2f} | GPU={gpu_temp:.2f} | "
            f"TADR={tadr_action} | AWS={aws_action}"
        )

        # ======================
        # Only send if:
        # 1) action != NO_ACTION
        # 2) cooldown passed
        # 3) not duplicate
        # ======================
        if (
            aws_action != "NO_ACTION"
            and allow_action()
            and aws_action != last_sent_action
        ):

            payload = {
                "cpu_temp": cpu_temp,
                "gpu_temp": gpu_temp,
                "tadr_action": tadr_action,
                "action": aws_action,
                "timestamp": time.time(),
                "instance_id": data.get("instance_id")
            }

            producer.send(OUTPUT_TOPIC, payload)
            last_sent_action = aws_action

            print(f"Sent action → {aws_action}")

    except Exception as e:
        print("Error:", e)
