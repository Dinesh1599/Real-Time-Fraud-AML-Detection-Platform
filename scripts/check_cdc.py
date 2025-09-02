import json, time, sys, os
from confluent_kafka import Consumer, KafkaException, KafkaError, admin

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")  # host access from your compose
TOPICS = os.getenv("CDC_TOPICS",
    "orcl.APPUSER.CUSTOMERS,orcl.APPUSER.ACCOUNTS,orcl.APPUSER.TRANSACTIONS,"
    "orcl.APPUSER.BRANCHES,orcl.APPUSER.MERCHANTS,orcl.APPUSER.DEVICES,"
    "orcl.APPUSER.GEOS,orcl.APPUSER.LOGINS,orcl.APPUSER.SANCTIONS,orcl.APPUSER.ALERTS"
).split(",")

MODE = os.getenv("CDC_MODE", "earliest")  # "earliest" to see snapshot; "latest" for new-only
MAX_WAIT_SEC = int(os.getenv("CDC_WAIT", "8"))  # per topic

def list_topics(bootstrap):
    a = admin.AdminClient({"bootstrap.servers": bootstrap})
    md = a.list_topics(timeout=5)
    return sorted(md.topics.keys())

def sample_one(topic):
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"cdc-check-{int(time.time()*1000)}",
        "auto.offset.reset": MODE,   # earliest or latest
        "enable.auto.commit": False,
    }
    c = Consumer(conf)
    try:
        c.subscribe([topic])
        deadline = time.time() + MAX_WAIT_SEC
        while time.time() < deadline:
            msg = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                # non-fatal: end of partition, etc.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            val = msg.value()
            key = msg.key()
            try:
                js = json.loads(val.decode("utf-8")) if val is not None else None
            except Exception:
                js = val.decode("utf-8", errors="ignore") if val else None
            print(f"\n[{topic}] key={key.decode() if key else None}")
            print(json.dumps(js, indent=2, ensure_ascii=False) if isinstance(js, dict) else js)
            return True
        print(f"[{topic}] no messages received within {MAX_WAIT_SEC}s (topic may be empty or snapshot still running)")
        return False
    finally:
        c.close()

if __name__ == "__main__":
    print(f"bootstrap: {BOOTSTRAP}")
    existing = list_topics(BOOTSTRAP)
    missing = [t for t in TOPICS if t not in existing]
    if missing:
        print("topics not found:", ", ".join(missing))
    else:
        print("all topics found.")

    had_data = False
    for t in TOPICS:
        if t in existing:
            ok = sample_one(t)
            had_data = had_data or ok

    sys.exit(0 if had_data else 2)
