import pyspark.sql.functions as f

from dlt_expectation_util.validation import validate_not_null, validate_ipv4


def test_validate_not_null(spark):
    test_cases = [
        ("1", "0", True),
        ("2", "1", True),
        ("3", "None", True),
        ("4", "NULL", True),
        ("5", "NoneType", True),
        ("6", "", True),
        ("7", None, False),
    ]
    
    results = (
        spark
        .createDataFrame(test_cases, "case_id STRING, value STRING, expected BOOLEAN")
        .withColumn("actual", validate_not_null("value"))
        .filter(f.col("actual") != f.col("expected"))
        .select("case_id")
    )
    assert results.count() == 0


def test_validate_ipv4(spark):
    test_cases = [
        ("1", "192.1.1.1", True),
        ("2", "0.0.0.0", True),
        ("3", "192", False),
        ("4", "256.0.0.0", False),
    ]
    
    results = (
        spark
        .createDataFrame(test_cases, "case_id STRING, value STRING, expected BOOLEAN")
        .withColumn("actual", validate_ipv4("value"))
        .filter(f.col("actual") != f.col("expected"))
        .select("case_id")
    )
    assert results.count() == 0



