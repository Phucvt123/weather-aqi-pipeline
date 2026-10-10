"""
verify_lakehouse.py — Phase 1 completion check

Run via spark-submit on the Spark cluster:
    docker exec lakehouse-spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/scripts/verify_lakehouse.py

Success criteria:
    1. Nessie catalog is reachable
    2. Can create namespace 'bronze'
    3. Can create Iceberg table with MERGE support
    4. Can write and read data
    5. MinIO has data files

After this script passes, verify from Trino:
    docker exec lakehouse-trino trino --execute \
        "SELECT * FROM iceberg.bronze.verify_test"
"""

from pyspark.sql import SparkSession
import sys


def main():
    print("=" * 60)
    print("  PHASE 1 VERIFICATION: Lakehouse Infrastructure")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("verify_lakehouse") \
        .getOrCreate()

    try:
        # Step 1: Create namespace
        print("\n[1/5] Creating namespace 'bronze'...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
        print("  ✓ Namespace 'bronze' created")

        # Step 2: Create Iceberg table
        print("\n[2/5] Creating Iceberg table 'bronze.verify_test'...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS nessie.bronze.verify_test (
                city_code STRING,
                timestamp_local TIMESTAMP,
                temperature_c DOUBLE,
                aqi_eu INT,
                ingested_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(ingested_at))
        """)
        print("  ✓ Table created with partition by days(ingested_at)")

        # Step 3: Insert test data
        print("\n[3/5] Inserting test data...")
        spark.sql("""
            INSERT INTO nessie.bronze.verify_test VALUES
            ('HN',  TIMESTAMP '2025-06-15 10:00:00', 32.5, 45, TIMESTAMP '2025-06-15 10:30:00'),
            ('HCM', TIMESTAMP '2025-06-15 10:00:00', 35.2, 78, TIMESTAMP '2025-06-15 10:30:00'),
            ('DN',  TIMESTAMP '2025-06-15 10:00:00', 30.1, 22, TIMESTAMP '2025-06-15 10:30:00')
        """)
        print("  ✓ 3 rows inserted")

        # Step 4: Read back and verify
        print("\n[4/5] Reading data back...")
        df = spark.sql("SELECT * FROM nessie.bronze.verify_test ORDER BY city_code")
        df.show()
        count = df.count()
        assert count == 3, f"Expected 3 rows, got {count}"
        print(f"  ✓ Read {count} rows successfully")

        # Step 5: Test MERGE INTO (core feature for fixing bug #1)
        print("\n[5/5] Testing MERGE INTO (forecast update scenario)...")
        spark.sql("""
            MERGE INTO nessie.bronze.verify_test t
            USING (
                SELECT 'HN' as city_code,
                       TIMESTAMP '2025-06-15 10:00:00' as timestamp_local,
                       33.0 as temperature_c,
                       50 as aqi_eu,
                       TIMESTAMP '2025-06-15 11:30:00' as ingested_at
            ) s
            ON t.city_code = s.city_code AND t.timestamp_local = s.timestamp_local
            WHEN MATCHED AND s.ingested_at > t.ingested_at THEN
                UPDATE SET t.temperature_c = s.temperature_c,
                           t.aqi_eu = s.aqi_eu,
                           t.ingested_at = s.ingested_at
            WHEN NOT MATCHED THEN
                INSERT *
        """)

        # Verify the merge updated HN's temperature
        result = spark.sql("""
            SELECT temperature_c, aqi_eu
            FROM nessie.bronze.verify_test
            WHERE city_code = 'HN'
        """).collect()

        assert result[0].temperature_c == 33.0, \
            f"MERGE failed: expected 33.0, got {result[0].temperature_c}"
        assert result[0].aqi_eu == 50, \
            f"MERGE failed: expected 50, got {result[0].aqi_eu}"
        print("  ✓ MERGE INTO works — forecast update scenario verified!")

        # Summary
        print("\n" + "=" * 60)
        print("  ✅ ALL CHECKS PASSED — Phase 1 infrastructure is ready!")
        print("=" * 60)
        print("\nNext steps:")
        print("  1. Verify Trino can read the table:")
        print('     docker exec lakehouse-trino trino --execute \\')
        print('       "SELECT * FROM iceberg.bronze.verify_test"')
        print("  2. Check MinIO console at http://localhost:9001")
        print("     (login: minioadmin / minioadmin123)")
        print("  3. Clean up test table:")
        print('     DROP TABLE nessie.bronze.verify_test')

    except Exception as e:
        print(f"\n  ❌ VERIFICATION FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
