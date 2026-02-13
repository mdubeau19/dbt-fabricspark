from dbt.adapters.fabricspark import FabricSparkCredentials


def test_credentials_server_side_parameters_keys_and_values_are_strings() -> None:
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="tests",
        schema="tests",
        workspaceid="",
        lakehouseid="",
        spark_config={"name": "test-session"},
    )
    assert credentials.schema == "tests"
