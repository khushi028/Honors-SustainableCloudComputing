import json
import time
import boto3
import logging
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# LOGGING SETUP (VERY IMPORTANT)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("aws-scaler")


# CONFIG

KAFKA_BROKER = "localhost:9092"
TOPIC = "scheduler-topic"

ASG_NAME = "scheduler-asg-kj"

COOLDOWN = 300
SCALE_STEP = 1

MIN_CAPACITY = 1
MAX_CAPACITY = 5


# AWS CLIENT

autoscaling = boto3.client("autoscaling")

last_action_time = 0


# HELPERS

def get_current_capacity():
    try:
        resp = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[ASG_NAME]
        )

        if not resp["AutoScalingGroups"]:
            log.error(f"ASG '{ASG_NAME}' NOT FOUND")
            return None

        group = resp["AutoScalingGroups"][0]
        cap = group["DesiredCapacity"]

        log.info(f"Current ASG capacity = {cap}")
        return cap

    except Exception as e:
        log.exception("Failed fetching capacity")
        return None


def set_capacity(new_capacity):
    try:
        autoscaling.set_desired_capacity(
            AutoScalingGroupName=ASG_NAME,
            DesiredCapacity=new_capacity,
            HonorCooldown=False
        )
        log.info(f"Capacity updated → {new_capacity}")

    except ClientError:
        log.exception("Scaling API failed")


def allow_action():
    global last_action_time

    now = time.time()
    diff = now - last_action_time

    if diff > COOLDOWN:
        last_action_time = now
        return True

    remaining = int(COOLDOWN - diff)
    log.info(f"Cooldown active → {remaining}s remaining")
    return False


# SCALING

def scale_up():
    log.info("Scale UP requested")

    current = get_current_capacity()
    if current is None:
        return

    if current >= MAX_CAPACITY:
        log.warning("Already at MAX capacity")
        return

    new_capacity = current + SCALE_STEP
    log.info(f"Scaling UP {current} → {new_capacity}")
    set_capacity(new_capacity)


def scale_down():
    log.info("Scale DOWN requested")

    current = get_current_capacity()
    if current is None:
        return

    if current <= MIN_CAPACITY:
        log.warning("Already at MIN capacity")
        return

    new_capacity = current - SCALE_STEP
    log.info(f"📉 Scaling DOWN {current} → {new_capacity}")
    set_capacity(new_capacity)


# KAFKA CONSUMER

log.info("Connecting to Kafka...")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="aws-scheduler-group"
)

log.info("Connected to Kafka")

# show partitions & offsets
partitions = consumer.partitions_for_topic(TOPIC)
log.info(f"Topic partitions: {partitions}")

log.info("Testing AWS connection...")
get_current_capacity()

log.info("AWS Scaler Started... Listening for actions")


# MAIN LOOP

for msg in consumer:
    try:
        log.info(f"Raw message → offset={msg.offset}")

        payload = msg.value
        log.info(f"Payload → {payload}")

        action = payload.get("action", "NO_ACTION")
        cpu = payload.get("cpu_temp")
        gpu = payload.get("gpu_temp")

        log.info(f"Decision → CPU={cpu} GPU={gpu} ACTION={action}")

        if action == "NO_ACTION":
            log.info("Skipping → NO_ACTION")
            continue

        if not allow_action():
            continue

        if action == "SCALE_UP":
            scale_up()

        elif action == "SCALE_DOWN":
            scale_down()

        else:
            log.warning(f"Unknown action: {action}")

    except Exception:
        log.exception("Message processing failed")
