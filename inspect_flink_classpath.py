import glob
import os
import zipfile

patterns = [
    "org/apache/flink/kafka/shaded/org/apache/kafka/clients/consumer/OffsetResetStrategy.class",
    "org/apache/kafka/clients/consumer/OffsetResetStrategy.class",
]

directories = [
    "/opt/flink/lib",
    "/opt/flink/opt",
]

for directory in directories:
    print(f"\n===== {directory} =====")

    for path in glob.glob(directory + "/**/*.jar", recursive=True):
        try:
            with zipfile.ZipFile(path) as jar:
                names = set(jar.namelist())
                matches = [pattern for pattern in patterns if pattern in names]

                if matches:
                    print(f"\nJAR: {os.path.basename(path)}")
                    for match in matches:
                        print(f"  FOUND: {match}")
        except Exception as exc:
            print(f"ERROR: {path}: {exc}")
