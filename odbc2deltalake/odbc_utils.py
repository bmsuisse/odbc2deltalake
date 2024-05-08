from typing import Union

ODBC_DRIVER: Union[str, None] = None

drivers: Union[list[str], None] = None


def build_connection_string(
    dict_dt: Union[dict, str],
    *,
    odbc: bool = False,
    odbc_driver: Union[str, None] = None,
):
    if isinstance(dict_dt, str) and not odbc:
        return dict_dt
    global drivers
    global ODBC_DRIVER
    if odbc and not odbc_driver and ODBC_DRIVER is None:
        import pyodbc

        drivers = drivers or pyodbc.drivers()
        prio = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server",
        ]
        for p in prio:
            if p in drivers:
                ODBC_DRIVER = p
                break
        if ODBC_DRIVER is None:
            ODBC_DRIVER = next(
                (d for d in drivers if "for SQL Server" in d),
                "ODBC Driver 17 for SQL Server",
            )
        assert ODBC_DRIVER is not None
    if isinstance(dict_dt, str):
        assert ODBC_DRIVER is not None
        return "DRIVER=" + (odbc_driver or ODBC_DRIVER) + ";" + dict_dt
    opts = dict_dt | {"DRIVER": (odbc_driver or ODBC_DRIVER)} if odbc else dict_dt
    return ";".join((k + "=" + str(v) for k, v in opts.items()))
