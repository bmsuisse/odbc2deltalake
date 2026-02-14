from odbc2deltalake.write_init import (
    IS_DELETED_COL_NAME,
    IS_FULL_LOAD_COL_NAME,
    VALID_FROM_COL_NAME,
    OPERATION_COL_NAME,
    DBDeltaPathConfigs,
    detect_operation_mode,
)
from odbc2deltalake.write_init import WriteConfigAndInfos
import sqlglot.expressions as ex
import sqlglot as sg
from typing import Union
from odbc2deltalake.sql_glot_utils import count_limit_one

table_name_type = Union[str, tuple[str, str], tuple[str, str, str]]


def _get_is_full_load_condition_restore_pk(
    operation_mode: str,
    table_alias: Union[str, None] = None,
) -> ex.Expression:
    """Get the WHERE condition to filter for full load records."""
    if operation_mode == "operation":
        return ex.column(OPERATION_COL_NAME, table_alias, quoted=True).eq("reload")
    else:
        return ex.column(IS_FULL_LOAD_COL_NAME, table_alias, quoted=True).eq(True)


def _get_is_deleted_condition_restore_pk(
    operation_mode: str,
    table_alias: Union[str, None] = None,
    negate: bool = False,
) -> ex.Expression:
    """Get the WHERE condition to filter for deleted records."""
    if operation_mode == "operation":
        condition = ex.column(OPERATION_COL_NAME, table_alias, quoted=True).eq("delete")
    else:
        condition = ex.column(IS_DELETED_COL_NAME, table_alias, quoted=True)
    
    if negate:
        return ~condition
    return condition


def create_last_pk_version_view(
    infos: WriteConfigAndInfos,
    view_prefix: str = "",
):
    assert len(infos.pk_cols) > 0, "must have at least one pk column"
    delta_path = infos.destination / "delta"
    reader = infos.source
    write_config = infos.write_config
    
    # Detect operation mode
    if write_config.operation_column_mode is not None:
        operation_mode = write_config.operation_column_mode
    else:
        operation_mode = detect_operation_mode(reader, delta_path)

    temp_table = "tmp_" + str(abs(hash(str(delta_path))))
    reader.local_register_update_view(delta_path, temp_table)

    sq_valid_from = reader.local_execute_sql_to_py(
        sg.from_(ex.to_identifier(temp_table))
        .select(
            ex.func("max", ex.column(VALID_FROM_COL_NAME, quoted=True)).as_(
                VALID_FROM_COL_NAME, quoted=True
            )
        )
        .where(_get_is_full_load_condition_restore_pk(operation_mode))
    )
    if sq_valid_from is None or len(sq_valid_from) == 0:
        return None, None, False
    assert infos.delta_col is not None, "must have a delta column"
    latest_full_load_date = sq_valid_from[0][VALID_FROM_COL_NAME]
    reader.local_register_view(
        sg.from_(ex.table_(ex.to_identifier(temp_table), alias="tr"))
        .select(
            *(
                [
                    ex.column(write_config.get_target_name(c), quoted=True)
                    for c in infos.pk_cols
                ]
                + [
                    ex.column(
                        write_config.get_target_name(infos.delta_col), quoted=True
                    ),
                    ex.column(VALID_FROM_COL_NAME, quoted=True),
                ]
            ),
            copy=False,
        )
        .where(
            _get_is_full_load_condition_restore_pk(operation_mode)
            and ex.column(VALID_FROM_COL_NAME, quoted=True).eq(
                ex.Subquery(
                    this=ex.select(
                        ex.func(
                            "MAX", ex.column(VALID_FROM_COL_NAME, "ts", quoted=True)
                        )
                    )
                    .from_(ex.table_(ex.to_identifier(temp_table), alias="ts"))
                    .where(_get_is_full_load_condition_restore_pk(operation_mode))
                )
            )
        ),
        view_prefix + "last_full_load",
    )

    # Determine which column to use for tracking
    deleted_col_name = OPERATION_COL_NAME if operation_mode == "operation" else IS_DELETED_COL_NAME
    
    sq = (
        sg.from_(ex.table_(temp_table, alias="tr"))
        .select(
            *(
                [
                    ex.column(write_config.get_target_name(c), "tr", quoted=True)
                    for c in infos.pk_cols
                ]
                + [
                    ex.column(
                        write_config.get_target_name(infos.delta_col), "tr", quoted=True
                    )
                ]
                + [ex.column(deleted_col_name, "tr", quoted=True)]
                + [ex.column(VALID_FROM_COL_NAME, "tr", quoted=True)]
            )
        )
        .where(
            ex.column(VALID_FROM_COL_NAME, quoted=True)
            > ex.convert(latest_full_load_date)
        )
    )
    sq = sq.qualify(
        ex.EQ(
            this=ex.Window(
                this=ex.RowNumber(),
                partition_by=[
                    ex.column(write_config.get_target_name(pk), quoted=True)
                    for pk in infos.pk_cols
                ],
                order=ex.Order(
                    expressions=[
                        ex.Ordered(
                            this=ex.column(VALID_FROM_COL_NAME, quoted=True),
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
        sg.from_(ex.table_("base", alias="b"))
        .where(_get_is_deleted_condition_restore_pk(operation_mode, table_alias="b", negate=True))
        .with_(
            "base",
            as_=ex.union(
                sg.from_(
                    ex.table_(view_prefix + "delta_after_full_load", alias="df")
                ).select(
                    *(
                        [
                            ex.column(
                                write_config.get_target_name(c), "df", quoted=True
                            )
                            for c in infos.pk_cols
                        ]
                        + [
                            ex.column(
                                write_config.get_target_name(infos.delta_col),
                                "df",
                                quoted=True,
                            ),
                            ex.column(deleted_col_name, "df", quoted=True),
                        ]
                    )
                ),
                sg.from_(ex.table_(view_prefix + "last_full_load", alias="f"))
                .select(
                    *(
                        [
                            ex.column(write_config.get_target_name(c), "f", quoted=True)
                            for c in infos.pk_cols
                        ]
                        + [
                            ex.column(
                                write_config.get_target_name(infos.delta_col),
                                "f",
                                quoted=True,
                            ),
                            (
                                ex.convert("upsert").as_(OPERATION_COL_NAME, quoted=True)
                                if operation_mode == "operation"
                                else ex.convert(False).as_(IS_DELETED_COL_NAME, quoted=True)
                            ),
                        ]
                    )
                )
                .join(
                    ex.table_(view_prefix + "delta_after_full_load", alias="d"),
                    join_type="anti",
                    on=ex.and_(
                        *[
                            ex.column(
                                write_config.get_target_name(c), "f", quoted=True
                            ).eq(
                                ex.column(
                                    write_config.get_target_name(c), "d", quoted=True
                                )
                            )
                            for c in infos.pk_cols
                        ]
                    ),
                ),
                distinct=False,
            ),
        )
        .select(
            *(
                [
                    ex.column(write_config.get_target_name(c), "b", quoted=True)
                    for c in infos.pk_cols
                ]
                + [
                    ex.column(
                        write_config.get_target_name(infos.delta_col),
                        "b",
                        quoted=True,
                    )
                ]
            ),
            append=False,
        )
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
        allow_schema_drift=True,
    )
    return True
