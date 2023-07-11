import pyspark.sql.functions as f


def validate_not_null(column_name):
    return f.col(column_name).isNotNull()


def validate_ipv4(column_name):
    return f.col(column_name).rlike(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
