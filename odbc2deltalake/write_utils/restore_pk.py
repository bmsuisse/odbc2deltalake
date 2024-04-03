from odbc2deltalake.db_to_delta import (
    IS_DELETED_COL_NAME,
    IS_FULL_LOAD_COL_NAME,
    VALID_FROM_COL_NAME,
    DBDeltaPathConfigs,
    WriteConfig,
)
from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.metadata import InformationSchemaColInfo
from odbc2deltalake.reader.reader import DataSourceReader
import sqlglot.expressions as ex
import sqlglot as sg

from odbc2deltalake.sql_glot_utils import count_limit_one

table_name_type = str | tuple[str, str] | tuple[str, str, str]


def _temp_table(table: table_name_type):
    if isinstance(table, str):
        return "temp_" + table
    return "temp_" + "_".join(table)


def restore_last_pk(
    reader: DataSourceReader,
    table: table_name_type,
    destination: Destination,
    delta_col: InformationSchemaColInfo,
    pk_cols: list[InformationSchemaColInfo],
    write_config: WriteConfig,
):
    delta_path = destination / "delta"
    reader.local_register_update_view(delta_path, _temp_table(table))

    sq_valid_from = reader.local_execute_sql_to_py(
        sg.from_(ex.to_identifier(_temp_table(table)))
        .select(ex.func("max", ex.column(VALID_FROM_COL_NAME)).as_(VALID_FROM_COL_NAME))
        .where(ex.column(IS_FULL_LOAD_COL_NAME).eq(True))
    )
    if sq_valid_from is None or len(sq_valid_from) == 0:
        return False
    latest_full_load_date = sq_valid_from[0][VALID_FROM_COL_NAME]
    reader.local_register_view(
        sg.from_(ex.table_(ex.to_identifier(_temp_table(table)), alias="tr"))
        .select(
            *(
                [ex.column(write_config.get_target_name(c), "tr") for c in pk_cols]
                + [ex.column(delta_col.column_name, "tr")]
            )
        )
        .where(
            ex.column(IS_FULL_LOAD_COL_NAME).eq(True)
            and ex.column(VALID_FROM_COL_NAME).eq(
                ex.Subquery(
                    this=ex.select(ex.func("MAX", ex.column(VALID_FROM_COL_NAME)))
                    .from_(ex.table_(ex.to_identifier(_temp_table(table)), alias="tr"))
                    .where(ex.column(IS_FULL_LOAD_COL_NAME).eq(True))
                )
            )
        ),
        "last_full_load",
    )

    sq = (
        sg.from_(ex.table_(_temp_table(table), alias="tr"))
        .select(
            *(
                [ex.column(write_config.get_target_name(c), "tr") for c in pk_cols]
                + [ex.column(write_config.get_target_name(delta_col), "tr")]
                + [ex.column(IS_DELETED_COL_NAME, "tr")]
            )
        )
        .where(ex.column(VALID_FROM_COL_NAME) > ex.convert(latest_full_load_date))
    )
    sq = sq.qualify(
        ex.EQ(
            this=ex.Window(
                this=ex.RowNumber(),
                partition_by=[
                    ex.column(write_config.get_target_name(pk)) for pk in pk_cols
                ],
                order=ex.Order(
                    expressions=[
                        ex.Ordered(
                            this=ex.column(write_config.get_target_name(delta_col)),
                            desc=True,
                            nulls_first=False,
                        )
                    ]
                ),
                over="OVER",
            ),
            expression=ex.convert(1),
        )
    )
    reader.local_register_view(sq, "delta_after_full_load")
    reader.local_register_view(
        sq.from_("base")
        .where(ex.column(IS_DELETED_COL_NAME))
        .with_(
            "base",
            as_=ex.union(
                left=sq.from_("delta_after_full_load").select("*"),
                right=sq.from_(ex.table_("last_full_load", "f")).join(
                    ex.table_("delta_after_full_load", "d"),
                    join_type="anti",
                    on=ex.and_(
                        *[
                            ex.column(write_config.get_target_name(c), "f").eq(
                                ex.column(write_config.get_target_name(c), "d")
                            )
                            for c in pk_cols
                        ]
                    ),
                ),
                distinct=False,
            ),
        )
        .select(ex.Star(**{"except": [ex.column(IS_DELETED_COL_NAME)]})),
        "v_last_pk_version",
    )
    cnt = reader.local_execute_sql_to_py(count_limit_one("v_last_pk_version"))[0]["cnt"]
    if cnt == 0:
        return False
    reader.local_execute_sql_to_delta(
        sq.from_("v_last_pk_version").select(ex.Star()),
        destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION,
        mode="overwrite",
    )
    return True
