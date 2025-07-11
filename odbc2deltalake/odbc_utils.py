from typing import Union


drivers: Union[list[str], None] = None

POSTGRES_ODBC_DRIVERS = ["PostgreSQL Unicode(x64)", "PostgreSQL Unicode"]


def build_connection_string(
    dict_dt: Union[dict, str],
    *,
    odbc: bool = False,
    odbc_driver: Union[str, list[str], None] = None,
):
    if isinstance(dict_dt, str) and not odbc:
        return dict_dt
    global drivers
    global ODBC_DRIVER
    selected_odbc_driver = (
        None if not odbc else odbc_driver if isinstance(odbc_driver, str) else None
    )
    if isinstance(odbc_driver, list) and len(odbc_driver) == 1:
        selected_odbc_driver = odbc_driver[0]
    if odbc and not odbc_driver:
        odbc_driver = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server",
        ]
    if odbc and not selected_odbc_driver:
        candidate_drivers = (
            odbc_driver
            if isinstance(odbc_driver, list)
            else [odbc_driver or "SQL Server"]
        )
        import pyodbc

        drivers = drivers or pyodbc.drivers()

        for p in candidate_drivers:
            if p in drivers:
                selected_odbc_driver = p
                break
        if selected_odbc_driver is None:
            selected_odbc_driver = candidate_drivers[0]
    if isinstance(dict_dt, str):
        return "DRIVER=" + (selected_odbc_driver or "SQL Server") + ";" + dict_dt
    opts = (
        dict_dt | {"DRIVER": (selected_odbc_driver or "SQL Server")}
        if odbc
        else dict_dt
    )
    return ";".join((k + "=" + str(v) for k, v in opts.items()))
