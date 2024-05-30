from typing import Any, TYPE_CHECKING
import sqlglot as sg
import sqlglot.expressions as ex

if TYPE_CHECKING:
    from .write_init import WriteConfigAndInfos


def get_local_delta_value_and_count(
    infos: "WriteConfigAndInfos",
) -> tuple[Any, int]:
    from odbc2deltalake.write_init import DBDeltaPathConfigs

    if (infos.destination / "delta_load" / DBDeltaPathConfigs.PRIMARY_KEYS_TS).exists():
        delta_path = (
            infos.destination / "delta_load" / DBDeltaPathConfigs.PRIMARY_KEYS_TS
        )
    else:
        delta_path = infos.destination / "delta"
    tmp_view_name = "temp_" + str(abs(hash(str(delta_path))))
    infos.source.local_register_update_view(delta_path, tmp_view_name)
    row = infos.source.local_execute_sql_to_py(
        sg.from_(ex.to_identifier(tmp_view_name)).select(
            (
                ex.func(
                    "MAX",
                    ex.column(infos.write_config.get_target_name(infos.delta_col)),
                ).as_("max_ts")
                if infos.delta_col
                else ex.convert(None).as_("max_ts")
            ),
            ex.Count(this=ex.Star()).as_("cnt"),
        )
    )[0]
    mt, cnt = row["max_ts"], row["cnt"]
    if isinstance(mt, bytearray):
        return bytes(mt), cnt
    return mt, cnt


def retrieve_source_ts_cnt(infos: "WriteConfigAndInfos"):
    pk_ts_col_select = infos.from_("t").select(
        (
            ex.func(
                "MAX",
                ex.column(infos.delta_col.column_name, quoted=True),
            ).as_("max_ts")
            if infos.delta_col
            else ex.convert(None).as_("max_ts")
        ),
        ex.Count(this=ex.Star()).as_("cnt"),
    )

    infos.logger.info(
        "Retrieve source delta value / count",
        sql=pk_ts_col_select.sql(infos.write_config.dialect),
    )
    row = infos.source.source_sql_to_py(sql=pk_ts_col_select)
    return row[0]["max_ts"], row[0]["cnt"]
