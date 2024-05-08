from odbc2deltalake.write_init import (
    IS_DELETED_COL_NAME,
    IS_FULL_LOAD_COL_NAME,
    VALID_FROM_COL_NAME,
    DBDeltaPathConfigs,
)
from odbc2deltalake.write_init import WriteConfigAndInfos
import sqlglot.expressions as ex
import sqlglot as sg
from typing import Union
from odbc2deltalake.sql_glot_utils import count_limit_one

table_name_type = Union[str, tuple[str, str], tuple[str, str, str]]


def create_last_pk_version_view(
    infos: WriteConfigAndInfos,
    view_prefix: str = "",
):
    assert len(infos.pk_cols) > 0, "must have at least one pk column"
    delta_path = infos.destination / "delta"
    reader = infos.source
    write_config = infos.write_config

    temp_table = "tmp_" + str(abs(hash(str(delta_path))))
    reader.local_register_update_view(delta_path, temp_table)

    sq_valid_from = reader.local_execute_sql_to_py(
        sg.from_(ex.to_identifier(temp_table))
        .select(ex.func("max", ex.column(VALID_FROM_COL_NAME)).as_(VALID_FROM_COL_NAME))
        .where(ex.column(IS_FULL_LOAD_COL_NAME).eq(True))
    )
    if sq_valid_from is None or len(sq_valid_from) == 0:
        return None, None, False
    assert infos.delta_col is not None, "must have a delta column"
    latest_full_load_date = sq_valid_from[0][VALID_FROM_COL_NAME]
    reader.local_register_view(
        sg.from_(ex.table_(ex.to_identifier(temp_table), alias="tr"))
        .select(
            *(
                [ex.column(write_config.get_target_name(c)) for c in infos.pk_cols]
                + [
                    ex.column(write_config.get_target_name(infos.delta_col)),
                    ex.column(VALID_FROM_COL_NAME),
                ]
            ),
            copy=False,
        )
        .where(
            ex.column(IS_FULL_LOAD_COL_NAME).eq(True)
            and ex.column(VALID_FROM_COL_NAME).eq(
                ex.Subquery(
                    this=ex.select(ex.func("MAX", ex.column(VALID_FROM_COL_NAME, "ts")))
                    .from_(ex.table_(ex.to_identifier(temp_table), alias="ts"))
                    .where(ex.column(IS_FULL_LOAD_COL_NAME).eq(True))
                )
            )
        ),
        view_prefix + "last_full_load",
    )

    sq = (
        sg.from_(ex.table_(temp_table, alias="tr"))
        .select(
            *(
                [
                    ex.column(write_config.get_target_name(c), "tr")
                    for c in infos.pk_cols
                ]
                + [ex.column(write_config.get_target_name(infos.delta_col), "tr")]
                + [ex.column(IS_DELETED_COL_NAME, "tr")]
                + [ex.column(VALID_FROM_COL_NAME, "tr")]
            )
        )
        .where(ex.column(VALID_FROM_COL_NAME) > ex.convert(latest_full_load_date))
    )
    sq = sq.qualify(
        ex.EQ(
            this=ex.Window(
                this=ex.RowNumber(),
                partition_by=[
                    ex.column(write_config.get_target_name(pk)) for pk in infos.pk_cols
                ],
                order=ex.Order(
                    expressions=[
                        ex.Ordered(
                            this=ex.column(VALID_FROM_COL_NAME),
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
    reader.local_register_view(sq, view_prefix + "delta_after_full_load")
    last_pk_query = (
        sg.from_("base")
        .where(~ex.column(IS_DELETED_COL_NAME))
        .with_(
            "base",
            as_=ex.union(
                left=sg.from_(
                    ex.table_(view_prefix + "delta_after_full_load", alias="df")
                ).select(
                    *(
                        [
                            ex.column(write_config.get_target_name(c), "df")
                            for c in infos.pk_cols
                        ]
                        + [
                            ex.column(
                                write_config.get_target_name(infos.delta_col), "df"
                            ),
                            ex.column(IS_DELETED_COL_NAME, "df"),
                        ]
                    )
                ),
                right=sg.from_(ex.table_(view_prefix + "last_full_load", alias="f"))
                .select(
                    *(
                        [
                            ex.column(write_config.get_target_name(c), "f")
                            for c in infos.pk_cols
                        ]
                        + [
                            ex.column(
                                write_config.get_target_name(infos.delta_col), "f"
                            ),
                            ex.convert(False).as_(IS_DELETED_COL_NAME),
                        ]
                    )
                )
                .join(
                    ex.table_(view_prefix + "delta_after_full_load", alias="d"),
                    join_type="anti",
                    on=ex.and_(
                        *[
                            ex.column(write_config.get_target_name(c), "f").eq(
                                ex.column(write_config.get_target_name(c), "d")
                            )
                            for c in infos.pk_cols
                        ]
                    ),
                ),
                distinct=False,
            ),
        )
        .select(ex.Star(**{"except": [ex.column(IS_DELETED_COL_NAME)]}), append=False)
    )
    reader.local_register_view(
        last_pk_query,
        view_prefix + "last_pk_version",
    )
    return last_pk_query, view_prefix + "last_pk_version", True


def restore_last_pk(infos: WriteConfigAndInfos):
    query, view_name, success = create_last_pk_version_view(
        infos=infos,
        view_prefix="v_odbc_load_",
    )
    if not success:
        return False
    assert query is not None
    assert view_name is not None
    infos.logger.info(
        "Restoring last pk version", sql=query.sql(infos.source.query_dialect)
    )

    cnt = infos.source.local_execute_sql_to_py(count_limit_one(view_name))[0]["cnt"]
    if cnt == 0:
        return False
    infos.source.local_execute_sql_to_delta(
        sg.from_(view_name).select(ex.Star()),
        infos.destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION,
        mode="overwrite",
    )
    return True
