# Troubleshooting Guide

Issues encountered during the build of this PoC, with root causes and fixes.
These are documented so anyone reproducing this project can avoid the same pitfalls.

---

## 1. Mosquitto Port Conflict (Port 1883 Already in Use)

**Symptom:**
```
Error response from daemon: ports are not available: exposing port TCP
0.0.0.0:1883 -> 127.0.0.1:0: listen tcp 0.0.0.0:1883: bind: address already in use
```

**Root cause:**
The host machine (kcsn0010) already runs a system Mosquitto broker on port 1883.
This is the same broker used by the existing IoT bridge scripts
(`mqtt_local_bridge.py`, `mqtt_transmit_fds.py`, etc.). Docker cannot bind
the same port to a containerized Mosquitto.

**Fix:**
Do not run a Dockerized Mosquitto at all. Remove the `mosquitto` service from
`docker-compose.yml` and use the existing system Mosquitto instead.

The sensor simulator and MQTT-Kafka bridge connect to the host's Mosquitto
via the host's IP address (see Section 2 below).

**Lesson learned:**
Before adding a service to Docker Compose, check if the port is already in
use: `ss -tlnp | grep <port>`. If another service is already running on that
port, decide whether to reuse it or remap the Docker port (e.g., `1884:1883`).

---

## 2. Docker Container Cannot Connect to Host Mosquitto

**Symptom:**
```
Connection failed: [Errno 111] Connection refused. Retrying in 5s...
```
The sensor-simulator container (using `network_mode: host`) could not connect
to Mosquitto on `localhost:1883`.

**Root cause (two issues):**

### 2a. Mosquitto Bound to 127.0.0.1 Only

The default Mosquitto configuration had no `listener` directive, so it
listened only on `127.0.0.1`. Even with `network_mode: host`, the Docker
container's loopback interface was isolated.

**Fix:**
Add a listener directive to Mosquitto config:
```bash
echo "listener 1883 0.0.0.0" | sudo tee -a /etc/mosquitto/mosquitto.conf
sudo systemctl restart mosquitto
```

**Verification:**
```bash
# Before fix:
ss -tlnp | grep 1883
# LISTEN  127.0.0.1:1883  ← only loopback

# After fix:
ss -tlnp | grep 1883
# LISTEN  0.0.0.0:1883    ← all interfaces
```

### 2b. Duplicate Listener Lines Crash Mosquitto

After running the `echo "listener ..." | sudo tee -a` command twice by
accident, the config file contained two identical `listener 1883 0.0.0.0`
lines. Mosquitto cannot bind the same port twice and crashed on restart:

```
Job for mosquitto.service failed because the control process exited with error code.
```

**Fix:**
Remove the duplicates and add just one:
```bash
sudo sed -i '/^listener 1883 0.0.0.0$/d' /etc/mosquitto/mosquitto.conf
echo "listener 1883 0.0.0.0" | sudo tee -a /etc/mosquitto/mosquitto.conf
sudo systemctl restart mosquitto
```

**Lesson learned:**
When appending to config files with `tee -a`, always check the file
afterwards (`cat /etc/mosquitto/mosquitto.conf`) to verify no duplicates.
Better yet, use `grep` to check before appending:
```bash
grep -q "listener 1883 0.0.0.0" /etc/mosquitto/mosquitto.conf || \
  echo "listener 1883 0.0.0.0" | sudo tee -a /etc/mosquitto/mosquitto.conf
```

### 2c. allow_anonymous Required

After adding `listener 1883 0.0.0.0`, Mosquitto started rejecting
connections with:
```
Failed to connect, reason code: Not authorized
```

When you add a `listener` directive, Mosquitto defaults to requiring
authentication. For a local PoC, anonymous access is acceptable.

**Fix:**
```bash
echo "allow_anonymous true" | sudo tee -a /etc/mosquitto/mosquitto.conf
sudo systemctl restart mosquitto
```

### 2d. Use Host IP, Not localhost

Even with `network_mode: host` and Mosquitto on `0.0.0.0`, the container
still couldn't connect via `localhost`. Testing from inside the container
revealed that only the host's actual IP address works:

```
127.0.0.1:1883     → FAILED: Connection refused
localhost:1883     → FAILED: Connection refused
192.168.29.44:1883 → SUCCESS
```

**Fix:**
Set `MQTT_BROKER: "192.168.29.44"` in the Docker Compose environment
for the sensor-simulator service.

For the mqtt-kafka-bridge (which uses the Docker `pipeline` network, not
`network_mode: host`), use `extra_hosts` to map `host.docker.internal`
to the host's gateway IP:
```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
environment:
  MQTT_BROKER: "host.docker.internal"
```

**Lesson learned:**
Docker containers have different networking depending on the mode:
- `network_mode: host`: shares the host's network stack, but `localhost`
  may still not work on all Linux setups. Use the host's actual IP.
- Default bridge network: completely isolated. Use `extra_hosts` with
  `host-gateway` to reach host services.
- Custom bridge network (like `pipeline`): containers can reach each
  other by name (e.g., `kafka:9092`), but not host services without
  `extra_hosts`.

---

## 3. Docker Compose `version` Attribute Warning

**Symptom:**
```
WARN[0000] the attribute `version` is obsolete, it will be ignored
```

**Root cause:**
Modern Docker Compose (v2+) no longer uses the `version` key in
`docker-compose.yml`. It was required in Docker Compose v1 to specify
the file format version (e.g., `version: "3.8"`).

**Fix:**
Remove the `version: "3.8"` line from `docker-compose.yml`.

---

## 4. PyFlink + Delta Lake Dependency Conflict (pyarrow)

**Symptom:**
```
ERROR: Cannot install apache-flink and deltalake because these package
versions have conflicting dependencies.

The conflict is caused by:
    deltalake 0.22.3 depends on pyarrow>=16
    apache-beam 2.48.0 depends on pyarrow<12.0.0 and >=3.0.0
```

**Root cause:**
This is a fundamental, unresolvable conflict in the Python dependency tree:

```
apache-flink 1.18.1
  └── apache-beam 2.43-2.48
       └── pyarrow < 10 (or < 12 at best)

deltalake 0.22.3
  └── pyarrow >= 16
```

No version of pyarrow satisfies both constraints. Even upgrading to Flink
1.20 doesn't help (apache-beam 2.56 still requires pyarrow < 15).

**Things we tried that did NOT work:**
1. `pyarrow==15.0.2` — deltalake needs >= 16
2. `pyarrow>=16` — apache-beam needs < 10 (or < 12 or < 15 depending on version)
3. Upgrading to Flink 1.20 — still uses apache-beam with pyarrow < 15

**Fix: Separate Flink and Delta Lake into different containers.**

Instead of one container running both PyFlink and deltalake, we split into:
- **flink-jobmanager / flink-taskmanager**: Runs PyFlink with apache-flink
  and its compatible pyarrow. Reads from Kafka, processes data, writes
  results to OUTPUT Kafka topics.
- **delta-writer**: Runs deltalake with its compatible pyarrow. Reads from
  output Kafka topics, writes to Delta Lake files.

This is actually a common production pattern — stream processors typically
write to Kafka, and specialized sink services handle storage.

**Updated requirements.txt for each:**

Flink (`src/flink_jobs/requirements.txt`):
```
apache-flink==1.18.1
apache-flink-libraries==1.18.1
```

Delta Writer (`src/delta_writer/requirements.txt`):
```
kafka-python-ng==2.2.2
deltalake==0.22.3
pyarrow>=16
pandas==2.2.3
```

**Lesson learned:**
PyFlink's dependency on apache-beam creates a "pyarrow ceiling" that
conflicts with many modern data libraries. When using PyFlink, plan to
keep it isolated and use Kafka as the interface to other services.

---

## 5. Kafka Image Not Found (bitnami/kafka:3.7)

**Symptom:**
```
Error response from daemon: failed to resolve reference
"docker.io/bitnami/kafka:3.7": not found
```

**Root cause:**
The `bitnami/kafka` image tags don't use abbreviated version numbers like
`3.7`. The Bitnami Kafka images have moved or changed their tagging scheme.

**Fix:**
Use the official Apache Kafka image instead:
```yaml
image: apache/kafka:3.9.0
```

Key differences from bitnami:

| | bitnami/kafka | apache/kafka |
|---|---|---|
| Env variable prefix | `KAFKA_CFG_` | `KAFKA_` |
| Data directory | `/bitnami/kafka` | `/var/lib/kafka/data` |
| Controller port | 9094 | 9093 |
| Cluster ID | Auto-generated | Must provide `CLUSTER_ID` |

---

## 6. pip `--break-system-packages` Not Recognized

**Symptom:**
```
no such option: --break-system-packages
```

**Root cause:**
The `--break-system-packages` flag was added in pip 23.0+ (Python 3.11+
externally-managed environments). The `flink:1.18` Docker image uses an
older Debian with an older pip that doesn't support this flag.

**Fix:**
Remove `--break-system-packages` from the Dockerfile:
```dockerfile
# Before (broken):
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt --break-system-packages

# After (fixed):
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt
```

The `--break-system-packages` flag is only needed on newer systems where
pip refuses to install packages system-wide to protect the OS's Python
packages. Inside a Docker container, there's no OS to protect — it's fine
to install globally.

---

## 7. Orphan Container Warning

**Symptom:**
```
WARN[0000] Found orphan containers ([mosquitto]) for this project.
```

**Root cause:**
A Mosquitto container was previously created by Docker Compose. After removing
the `mosquitto` service from `docker-compose.yml`, the old container still
exists but is no longer managed by the Compose file.

**Fix:**
```bash
docker compose down --remove-orphans
docker compose up -d kafka sensor-simulator mqtt-kafka-bridge
```

---

## 8. paho-mqtt 2.x Callback API Changes

**Context (not an error we hit, but important to know):**

The existing IoT bridge scripts use paho-mqtt 1.x callback style:
```python
# paho-mqtt 1.x (old — used in mqtt_local_bridge.py etc.)
def on_connect(client, userdata, flags, rc):
    ...
client = mqtt.Client()
```

Our new code uses paho-mqtt 2.x:
```python
# paho-mqtt 2.x (new — used in simulator.py, bridge.py)
def on_connect(client, userdata, flags, reason_code, properties):
    ...
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
```

Key differences:
- Constructor requires `mqtt.CallbackAPIVersion.VERSION2`
- `on_connect` has 5 parameters instead of 4 (added `properties`)
- `rc` is now `reason_code` (an enum, not an int)

If you mix versions, you'll get confusing callback errors.

---

## Quick Reference: Useful Debug Commands

```bash
# Check what's listening on a port
ss -tlnp | grep <port>

# Check if a process is running
ps aux | grep <process_name>

# View Mosquitto config
cat /etc/mosquitto/mosquitto.conf

# Restart system Mosquitto
sudo systemctl restart mosquitto
sudo systemctl status mosquitto --no-pager

# Test MQTT connectivity
mosquitto_sub -h <broker_ip> -t <topic> -C 3

# List Kafka topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list

# Read messages from a Kafka topic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic <topic> --from-beginning --max-messages 3

# Check Docker container networking
docker exec <container> python -c "
import socket
s = socket.socket()
s.settimeout(2)
try:
    s.connect(('hostname', port))
    print('SUCCESS')
except Exception as e:
    print(f'FAILED: {e}')
s.close()
"

# View container logs
docker compose logs -f <service_name>

# Check container network mode
docker inspect <container> --format '{{.HostConfig.NetworkMode}}'

# Check Flink Web UI
# Open http://localhost:8081 in browser

# Inspect Delta Lake table
python -c "
from deltalake import DeltaTable
dt = DeltaTable('./data/delta/bronze_fds')
print(dt.schema())
print(dt.to_pandas().head())
"
```
