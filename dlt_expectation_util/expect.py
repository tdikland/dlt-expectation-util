from .validation import validate_not_null, validate_ipv4

try:
    import dlt # type: ignore
except ImportError:
    from . import fake_dlt as dlt

def expect_not_null(column_name):
    return dlt.expect(f"{column_name} is not null", validate_not_null(column_name))

def expect_ipv4(column_name):
    return dlt.expect(f"{column_name} is a valid ipv4 address", validate_ipv4(column_name))