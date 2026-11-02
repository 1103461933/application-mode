import zipfile

paths = [
    "/opt/flink/lib/flink-connector-kafka-5.0.0-2.2.jar",
    "/opt/flink/lib/kafka-clients-4.2.0.jar",
    "/opt/flink/opt/flink-python-2.2.0.jar",
]

patterns = [
    "OffsetResetStrategy.class",
    "KafkaSource.class",
    "KafkaOffsetsInitializer",
]

for path in paths:
    print(f"\n--- {path} ---")
    try:
        with zipfile.ZipFile(path) as jar:
            matches = [
                name for name in jar.namelist()
                if any(pattern in name for pattern in patterns)
            ]
            for name in matches[:30]:
                print(name)
            if not matches:
                print("No matching classes found")
    except Exception as exc:
        print(f"ERROR: {exc}")
