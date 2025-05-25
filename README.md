# spark-catalyst-hsm-example

> Spark Catalyst Extension for AES-GCM Encryption/Decryption using HSM-simulated Keys

This project demonstrates how to implement **custom cryptographic functions** in Apache Spark by extending the Catalyst Optimizer. The resulting JAR can be used as a library to enable SQL and DataFrame-based encryption/decryption operations inside both **Scala Spark** and **PySpark** environments.

The implementation supports:
- AES-GCM 128-bit encryption with randomly generated IVs
- Base64-encoded output for safe storage/transmission
- Secure key handling via an HSM simulator
- Spark Catalyst integration with support for WholeStage Code Generation

## WARNING NOTE:

This project was created as part of an educational article comparing the performance of different encryption implementations in PySpark:

- **Spark Catalyst Custom Functions (this implementation)**
- **Pandas UDFs**
- **Python UDFs**

It is **not intended for production use**, but rather to serve as a functional and illustrative example of how to extend Spark's Catalyst Optimizer with custom cryptographic expressions. While best practices were followed where possible, this code should be considered **educational and experimental** — it is not production-ready or feature-complete.

The goal is to demonstrate:
- How to create high-performance custom functions in Spark via Catalyst extensions
- Integration with WholeStage Code Generation for low-level optimization
- Use of secure AES-GCM encryption patterns inside Spark executors
- Comparison against Python-based alternatives like Pandas UDFs or Python UDFs

The full benchmarking suite that runs this `.jar` alongside Python-based UDFs can be found at:  
[spark-catalyst-udf-comparison](https://github.com/wiklich/spark-catalyst-udf-comparison )

That repository includes:
- CLI interface for running benchmarks
- Metrics collection via `sparkmeasure`
- Comparison reports between execution strategies

Its goal is to compare:
- Performance of WholeStage CodeGen-enabled Catalyst functions
- Pandas UDFs with vectorized execution
- Classic Python UDFs with serialization overhead

All results are intended for benchmarking and educational comparison only.

---

## Supported Functions

| Function | Description |
|---------|-------------|
| `encrypt_with_key(text)` | Encrypts a string using AES-GCM with a predefined key (`key_generic`) |
| `decrypt_with_key(ciphertext)` | Decrypts base64-encoded AES-GCM encrypted text |

These functions are injected into Spark's query engine via a custom extension and can be used directly in SQL queries or DataFrame operations.

---

## Technical Overview

### Architecture

This project is structured as follows:

```
src/
└── main/
    └── scala/
        └── br.com.wiklich.security.crypto/
            ├── CryptoExtension.scala     # Registers UDFs in Catalyst
            ├── EncryptWithKey.scala      # Public API for encrypt_with_key
            ├── EncryptWithKeyExpression.scala # Core encryption logic + codegen
            ├── DecryptWithKey.scala      # Public API for decrypt_with_key
            ├── DecryptWithKeyExpression.scala # Core decryption logic + codegen
        └── br.com.wiklich.security/
            └── HsmSimulator.scala        # Simulates secure key retrieval from HSM
```

### Cryptographic Implementation

- Uses **AES-GCM mode**: provides confidentiality and integrity (authenticated encryption).
- Each ciphertext includes:
  - A 12-byte Initialization Vector (IV)
  - The GCM-encrypted payload
- Output is encoded in **Base64** for safe transmission/storage.
- Keys are retrieved through `HsmSimulator`, simulating access to a real Hardware Security Module (HSM).

### Spark Integration

- Custom expressions are registered via `SparkSessionExtensions`.
- Both interpreted and **code-generated** paths are implemented:
  - `nullSafeEval`: fallback evaluation
  - `doGenCode`: generates Java code compiled at runtime for performance optimization
- Works seamlessly in both **DataFrame DSL** and **SQL queries**

---

## Build Instructions

### Prerequisites

- [SBT](https://www.scala-sbt.org/)
- Java 8 or higher
- Scala 2.12.x

### Compile and Package

Run the following command to build the JAR:

```bash
sbt clean assembly
```

The final JAR file will be located at:

```
target/scala-2.12/spark-catalyst-crypto.jar
```

---

## Usage with PySpark

To use this extension in PySpark:

### 1. Start Spark Session with Extension

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.extensions", "br.com.wiklich.security.crypto.CryptoExtension") \
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .getOrCreate()
```

Or optionally you can include the call to this extension in spark-submit as below:

```bash
spark-submit \
  --conf spark.sql.extensions=br.com.wiklich.security.crypto.CryptoExtension
  ...
```

### 2. Use the Functions in SQL

```python
spark.createDataFrame([("Alice",), ("Bob",)], ["name"]).createOrReplaceTempView("people")

result = spark.sql("""
    SELECT name, encrypt_with_key(name) AS encrypted_name FROM people
""").withColumnRenamed("encrypted_name", "encrypted")

result.show(truncate=False)
```

### 3. Decrypt Later

```python
decrypted = spark.sql("""
    SELECT encrypted_name, decrypt_with_key(encrypted_name) AS decrypted_name FROM (
        SELECT encrypt_with_key(name) AS encrypted_name FROM people
    )
""")
decrypted.show(truncate=False)
```

---

## Sample Output

| name  | encrypted_name (base64) | decrypted_name |
|-------|--------------------------|----------------|
| Alice | AwAAACAAAA...           | Alice          |
| Bob   | AwAAABAAAA...           | Bob            |

---

## Notes on Production Use

This example uses a simulated HSM (`HsmSimulator`) for development purposes. In production:

- Replace `HsmSimulator` with a real HSM or KMS integration (e.g., AWS KMS, Thales Luna, etc.)
- Add proper error handling and logging
- Support multiple keys and dynamic key rotation
- Consider adding unit tests using Spark testing frameworks

---