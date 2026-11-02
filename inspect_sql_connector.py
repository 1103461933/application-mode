import zipfile

path = "/tmp/flink-sql-connector-kafka.jar"
target = "org/apache/flink/kafka/shaded/org/apache/kafka/clients/consumer/OffsetResetStrategy.class"

with zipfile.ZipFile(path) as jar:
    names = jar.namelist()

    print(
        "SHADED CLASS FOUND"
        if target in names
        else "SHADED CLASS NOT FOUND"
    )

    matches = [
        name for name in names
        if name.endswith("OffsetResetStrategy.class")
    ]

    print("Matching classes:")
    for name in matches:
        print(name)
