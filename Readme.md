Thermal-Aware AWS Auto Scaling Scheduler

Predict server temperature in real-time and automatically scale EC2 instances using Kafka + Stream Processing + AWS Auto Scaling.

System dynamically:

monitors CPU/GPU temperature

predicts thermal stress

publishes scaling decisions

scales EC2 VMs up/down automatically

📦 Architecture
Producer (sensor data)
        ↓
Kafka Topic (scheduler-topic)
        ↓
Stream Processor (temperature + decision engine)
        ↓
Kafka Decision Message
        ↓
AWS Scaler (consumer)
        ↓
Auto Scaling Group (EC2 instances)

🧩 Components
1️⃣ Producer

Sends telemetry:

CPU temp
GPU temp
Power metrics
Weather data


→ Kafka

2️⃣ Stream Processor

Consumes metrics and decides:

SCALE_UP
SCALE_DOWN
NO_ACTION


Publishes:

{
  "action": "SCALE_UP",
  "cpu_temp": 45,
  "gpu_temp": 41
}


→ scheduler-topic

3️⃣ AWS Scaler

Kafka consumer that:

reads decisions

checks cooldown

updates ASG capacity using boto3

⚙️ AWS Setup Guide
Step 1 — Create Auto Scaling Group

EC2 → Auto Scaling Groups → Create

Launch template

AMI: ECS Optimized Linux 2 / Ubuntu

Instance type: t2.micro (free tier) or t3.large (prod)

Security group: allow app ports

ASG config
Name: thermal-asg
Min: 1
Desired: 1
Max: 5

Step 2 — Configure AWS credentials
aws configure


Enter:

Access Key
Secret Key
Region (IMPORTANT)


Example:

ap-south-1

Step 3 — Set region in code (REQUIRED)
autoscaling = boto3.client(
    "autoscaling",
    region_name="ap-south-1"
)


If region mismatches → ASG NOT FOUND error.

⚙️ Kafka Setup
Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

Start Kafka
kafka-server-start.sh config/server.properties

Create topic
kafka-topics.sh --create \
  --topic scheduler-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

▶️ Run Order (IMPORTANT)

Start in this order:

1. AWS scaler
python aws_scaler.py

2. Stream processor
python stream_processor.py

3. Producer
python producer.py


If order is wrong → consumer may miss messages.

🧠 Kafka Consumer Config (Best Practice)
consumer = KafkaConsumer(
    "scheduler-topic",
    bootstrap_servers="localhost:9092",
    group_id="aws-scheduler-group",
    auto_offset_reset="latest",
    enable_auto_commit=True
)

📊 Scaling Logic
if high temp  → SCALE_UP
if low usage → SCALE_DOWN
else         → NO_ACTION


Cooldown prevents rapid scaling:

COOLDOWN = 300s

🔍 Troubleshooting Guide
❌ AWS Scaler prints nothing
Cause

Consumer started AFTER messages.

Fix

Start scaler FIRST or use:

auto_offset_reset="earliest"

❌ ASG 'thermal-asg' NOT FOUND
Cause

Wrong region or name

Fix
aws autoscaling describe-auto-scaling-groups \
--query "AutoScalingGroups[].AutoScalingGroupName"


Set region in boto3:

region_name="ap-south-1"

❌ Kafka shows no messages
Check topic
kafka-topics.sh --list

Test manually
kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic scheduler-topic --from-beginning

❌ Consumer not receiving
Add group_id

Without it offsets break

group_id="aws-scheduler-group"

❌ Cooldown blocking scaling

Logs show:

Cooldown active → XXs remaining


Wait or reduce:

COOLDOWN = 60

📈 Example Logs (Healthy)
Connected to Kafka
Current ASG capacity = 1

Raw message → offset=3
Decision → ACTION=SCALE_UP
Scaling UP 1 → 2
Capacity updated → 2

🛠 Requirements
python 3.9+
kafka-python
boto3


Install:

pip install kafka-python boto3

🚀 Result

System now provides:

✅ real-time scaling
✅ cost optimization
✅ thermal protection
✅ automatic VM management

📌 Future Improvements

ECS task-per-VM scheduling

smarter hysteresis contro