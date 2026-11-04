"""Download and verify the immutable official Apache Kafka 4.0.1 release."""

import hashlib
import tarfile
import urllib.request
from pathlib import Path

name = "kafka_2.13-4.0.1.tgz"
# https://archive.apache.org/dist/kafka/4.0.1/kafka_2.13-4.0.1.tgz.sha512
expected = (
    "23b241098672d828ca86a6dd1dd99102fcdb9de8680ecf21"
    "3b47251a7ae7a342343cf41adf6fee9f920d171ae13f94c3"
    "9475165f5a246e1a46609a7d9d5a6811"
)
urllib.request.urlretrieve(f"https://archive.apache.org/dist/kafka/4.0.1/{name}", name)
actual = hashlib.file_digest(open(name, "rb"), "sha512").hexdigest()
if actual != expected:
    raise ValueError(f"Apache Kafka SHA-512 mismatch: {actual}")
with tarfile.open(name) as archive:
    archive.extractall("/opt", filter="data")
Path(name).unlink()
print(f"Verified Kafka 4.0.1 SHA-512: {actual}")
