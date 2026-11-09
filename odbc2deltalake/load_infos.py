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
    # MAX over the delta column as the load writes it (e.g. a rowversion cast to bigint), so
    # it compares with get_local_delta_value_and_count and the "no updates" check can match
    from .db_to_delta import _get_type_map, _source_convert

    delta_col = infos.delta_col
    pk_ts_col_select = infos.from_("t").select(
        (
            ex.func(
                "MAX",
                _source_convert(
                    delta_col.column_name,
                    delta_col.data_type,
                    delta_col.data_type_str,
                    table_alias="t",
                    type_map=_get_type_map(infos.write_config),
                    no_trim=infos.write_config.no_trim,
                ),
            ).as_("max_ts")
            if delta_col
            else ex.convert(None).as_("max_ts")
        ),
        ex.Count(this=ex.Star()).as_("cnt"),
    )

    infos.logger.info(
        "Retrieve source delta value / count",
        sql=pk_ts_col_select.sql(infos.write_config.dialect),
    )
    row = infos.source.source_sql_to_py(sql=pk_ts_col_select)
    mt = row[0]["max_ts"]
    return (bytes(mt) if isinstance(mt, bytearray) else mt), row[0]["cnt"]
