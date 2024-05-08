import dataclasses
from datetime import datetime, timezone
from typing import Callable, Iterable, Literal, Mapping, Optional, Sequence, TypeVar
import sqlglot as sg
from .utils import is_pydantic_2
from odbc2deltalake.sql_schema import is_string_type
from .utils import concat_seq
from odbc2deltalake.destination.destination import (
    Destination,
)
from odbc2deltalake.reader import DataSourceReader
from .query import sql_quote_name
from .metadata import (
    table_name_type,
    InformationSchemaColInfo,
)
import json
import sqlglot.expressions as ex
from .sql_glot_utils import table_from_tuple, union, count_limit_one
from .delta_logger import DeltaLogger
from .write_init import (
    WriteConfig,
    WriteConfigAndInfos,
    IS_DELETED_COL_NAME,
    IS_FULL_LOAD_COL_NAME,
    VALID_FROM_COL_NAME,
    DBDeltaPathConfigs,
)
from typing import Union

T = TypeVar("T")


def _source_convert(
    name: str,
    data_type: ex.DataType,
    *,
    table_alias: Union[str, None] = None,
    type_map: Optional[Mapping[str, ex.DataType]] = None,
):
    expr = ex.column(name, table_alias, quoted=True)
    if data_type.this in [
        ex.DataType.Type.ROWVERSION,
        ex.DataType.Type.TIMESTAMP,
    ]:  # can be removed after https://github.com/tobymao/sqlglot/issues/3345 is fixed
        data_type_str = "rowversion"
    else:
        data_type_str = (
            data_type
            if isinstance(data_type, str)
            else ex.DataType(this=data_type.this).sql("tsql").lower()
        )
    mapped_type = type_map.get(data_type_str) if type_map else None
    if mapped_type:
        expr = ex.cast(expr, mapped_type)
    if is_string_type(mapped_type or data_type):
        expr = ex.func("TRIM", expr)
    return expr


valid_from_expr = ex.cast(
    ex.func("GETUTCDATE", dialect="tsql"), ex.DataType(this="datetime2(6)")
).as_(VALID_FROM_COL_NAME)


def _get_cols_select(
    cols: Sequence[InformationSchemaColInfo],
    *,
    is_deleted: Union[bool, None] = None,
    is_full: Union[bool, None] = None,
    with_valid_from: bool = False,
    table_alias: Union[str, None] = None,
    system: Literal["source", "target"],
    data_type_map: Optional[Mapping[str, ex.DataType]] = None,
    get_target_name: Optional[Callable[[InformationSchemaColInfo], str]],
) -> Sequence[ex.Expression]:
    if get_target_name is None:
        get_target_name = lambda c: c.column_name

    return (
        [
            (
                _source_convert(
                    c.column_name,
                    c.data_type,
                    table_alias=table_alias,
                    type_map=data_type_map,
                ).as_(get_target_name(c), quoted=True)
                if system == "source"
                else ex.column(get_target_name(c), table_alias, quoted=True)
            )
            for c in cols
        ]
        + ([valid_from_expr] if with_valid_from else [])
        + (
            [ex.cast(ex.convert(int(is_deleted)), "bit").as_(IS_DELETED_COL_NAME)]
            if is_deleted is not None
            else []
        )
        + (
            [ex.cast(ex.convert(int(is_full)), "bit").as_(IS_FULL_LOAD_COL_NAME)]
            if is_full is not None
            else []
        )
    )


def _vacuum(source: DataSourceReader, dest: Destination):
    if source.local_delta_table_exists(dest):
        source.get_local_delta_ops(dest).vacuum()


def _transform_dt(dt: dict):
    dt["data_type"] = dt["data_type"].sql()
    return dt


def exec_write_db_to_delta(infos: WriteConfigAndInfos):
    write_config = infos.write_config
    cols = infos.col_infos
    pk_cols = infos.pk_cols
    destination = infos.destination
    source = infos.source
    delta_path = destination / "delta"
    dest_logger = infos.logger
    delta_col = infos.delta_col
    (destination / "meta").mkdir()
    (destination / "meta/schema.json").upload_str(
        json.dumps(
            [
                _transform_dt(c.model_dump() if is_pydantic_2 else c.dict())
                for c in cols
            ],
            indent=4,
        )
    )
    if source.local_delta_table_exists(
        destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
    ):
        try:
            last_version_pk = source.get_local_delta_ops(
                destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
            ).version()
        except Exception as e:
            import traceback

            dest_logger.warning(
                f"Could not get last version: {e}",
                error_trackback=traceback.format_exc(),
            )
            last_version_pk = None
    else:
        last_version_pk = None
    lock_file_path = destination / "meta/lock.txt"

    try:
        if (
            lock_file_path.exists()
            and (
                datetime.now(tz=timezone.utc) - lock_file_path.modified_time()
            ).total_seconds()
            > 60 * 60
        ):
            lock_file_path.remove()
        lock_file_path.upload_str("")

        if (
            not source.local_delta_table_exists(delta_path)
            or write_config.load_mode == "overwrite"
        ):
            delta_path.mkdir()
            do_full_load(infos=infos, mode="overwrite")
        elif write_config.load_mode == "append_inserts":
            if delta_col is None and len(pk_cols) == 1 and pk_cols[0].is_identity:
                delta_col = pk_cols[0]  # identity columns are usually increasing
                infos = dataclasses.replace(infos, delta_col=delta_col)
            assert (
                delta_col is not None
            ), "Must provide delta column for append_inserts load"
            do_append_inserts_load(infos)
        else:
            if (
                delta_col is None
                or len(pk_cols) == 0
                or write_config.load_mode == "force_full"
            ):
                do_full_load(
                    infos=infos,
                    mode="append",
                )
            else:
                do_delta_load(
                    infos=infos,
                    simple=write_config.load_mode
                    in ["simple_delta", "simple_delta_check"],
                    simple_check=write_config.load_mode == "simple_delta_check",
                )
        lock_file_path.remove()
        _vacuum(
            source, destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
        )
        _vacuum(source, destination / "delta_load" / DBDeltaPathConfigs.DELTA_1_NAME)
        _vacuum(source, destination / "delta_load" / DBDeltaPathConfigs.DELTA_2_NAME)
        _vacuum(source, destination / "delta_load" / DBDeltaPathConfigs.PRIMARY_KEYS_TS)
    except Exception as e:
        # restore files
        if last_version_pk is not None:
            o = source.get_local_delta_ops(
                destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
            )
            if o.version() > last_version_pk:
                o.restore(last_version_pk)
        import traceback

        dest_logger.error(
            "Error during load: {e}", error_trackback=traceback.format_exc()
        )
        raise e
    finally:
        if lock_file_path.exists():
            lock_file_path.remove()
        dest_logger.flush()


def write_latest_pk(
    reader: DataSourceReader,
    destination: Destination,
    pks: Sequence[InformationSchemaColInfo],
    delta_col: InformationSchemaColInfo,
    write_config: WriteConfig,
    merge_delta=False,
):
    reader.local_register_update_view(
        destination / f"delta_load/{DBDeltaPathConfigs.DELTA_1_NAME}",
        DBDeltaPathConfigs.DELTA_1_NAME,
    )

    reader.local_register_update_view(
        destination / f"delta_load/{DBDeltaPathConfigs.DELTA_2_NAME}",
        DBDeltaPathConfigs.DELTA_2_NAME,
    )
    if not merge_delta:
        reader.local_register_update_view(
            destination / f"delta_load/{DBDeltaPathConfigs.PRIMARY_KEYS_TS}",
            "primary_keys_ts_for_write",
        )
    else:
        reader.local_register_update_view(
            destination / f"delta_load/{DBDeltaPathConfigs.LATEST_PK_VERSION}",
            "primary_keys_ts_for_write",
            version=1,
        )

    latest_pk_query = union(
        [
            ex.select(
                *_get_cols_select(
                    cols=concat_seq(pks, [delta_col]),
                    table_alias="au",
                    system="target",
                    get_target_name=write_config.get_target_name,
                )
            ).from_(table_from_tuple("delta_2", alias="au")),
            (
                ex.select(
                    *_get_cols_select(
                        cols=concat_seq(pks, [delta_col]),
                        table_alias="d1",
                        system="target",
                        get_target_name=write_config.get_target_name,
                    )
                )
                .from_(ex.table_(DBDeltaPathConfigs.DELTA_1_NAME, alias="d1"))
                .join(
                    ex.table_("delta_2", alias="au2"),
                    ex.and_(
                        *[
                            ex.column(
                                write_config.get_target_name(c), "d1", quoted=True
                            ).eq(
                                ex.column(
                                    write_config.get_target_name(c), "au2", quoted=True
                                )
                            )
                            for c in pks
                        ]
                    ),
                    join_type="anti",
                )
            ),
            (
                ex.select(
                    *_get_cols_select(
                        cols=concat_seq(pks, [delta_col]),
                        table_alias="cpk",
                        system="target",
                        get_target_name=write_config.get_target_name,
                    )
                )
                .from_(ex.table_("primary_keys_ts_for_write", alias="cpk"))
                .join(
                    ex.table_("delta_2", alias="au3"),
                    ex.and_(
                        *[
                            ex.column(
                                write_config.get_target_name(c), "cpk", quoted=True
                            ).eq(
                                ex.column(
                                    write_config.get_target_name(c), "au3", quoted=True
                                )
                            )
                            for c in pks
                        ]
                    ),
                    join_type="anti",
                )
                .join(
                    ex.table_(DBDeltaPathConfigs.DELTA_1_NAME, alias="au4"),
                    ex.and_(
                        *[
                            ex.column(
                                write_config.get_target_name(c), "cpk", quoted=True
                            ).eq(
                                ex.column(
                                    write_config.get_target_name(c), "au4", quoted=True
                                )
                            )
                            for c in pks
                        ]
                    ),
                    join_type="anti",
                )
            ),
        ],
        distinct=False,
    )
    reader.local_execute_sql_to_delta(
        latest_pk_query,
        destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION,
        mode="overwrite",
    )


def _temp_table(table: Union[table_name_type, ex.Query]):
    if isinstance(table, ex.Query):
        return "temp_" + str(abs(hash(table.sql("duckdb"))))

    def _clean(input_str: str):
        return "".join(ch for ch in input_str if ch.isalnum())

    if isinstance(table, str):
        return "temp_" + _clean(table)
    return "temp_" + "_".join((_clean(s) for s in table))


def do_delta_load(
    infos: WriteConfigAndInfos,
    simple=False,  # a simple delta load assumes that there are no deletes and no additional updates (eg, when soft-delete is implemented in source properly)
    simple_check=False,  # does a simple load and checks if the source and target counts match. If not, do a normal delta load on top
):
    destination = infos.destination
    logger = infos.logger
    delta_col = infos.delta_col
    write_config = infos.write_config
    assert delta_col is not None, "Must have a delta_col for delta loads"
    reader = infos.source
    last_pk_path = (
        destination / f"delta_load/{DBDeltaPathConfigs.LATEST_PK_VERSION}"
        if not simple
        else None
    )
    logger.info(
        f"Start { 'SIMPLE ' if simple else '' }Delta Load with Delta Column {delta_col.column_name} and pks: {', '.join((c.column_name for c in infos.pk_cols))}"
    )

    if last_pk_path and not reader.local_delta_table_exists(
        last_pk_path
    ):  # or do a full load?
        logger.warning("Primary keys missing, try to restore")
        try:
            from .write_utils.restore_pk import restore_last_pk

            restore_success = restore_last_pk(infos=infos)
        except Exception as e:
            logger.warning(f"Could not restore primary keys: {e}")
            restore_success = False
        if not restore_success:
            logger.warning("No primary keys found, do a full load")
            do_full_load(infos=infos, mode="append")
            return
    old_pk_version = (
        reader.get_local_delta_ops(
            destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
        ).version()
        if not simple
        else None
    )
    delta_path = destination / "delta"
    delta_load_value = _get_latest_delta_value(
        reader, delta_path, delta_col, write_config
    )

    if delta_load_value is None:
        logger.warning("No delta load value, do a full load")
        do_full_load(
            infos=infos,
            mode="append",
        )
        return
    logger.info(
        "Start delta step 1, get primary keys and timestamps. MAX({delta_col.column_name}): {delta_load_value}"
    )
    if not simple:
        _retrieve_primary_key_data(infos=infos)
    else:
        source_count = reader.source_sql_to_py(
            infos.from_("t").select(ex.Count(this=ex.Star()).as_("cnt"))
        )[0]["cnt"]
    criterion = _source_convert(
        delta_col.column_name,
        delta_col.data_type,
        table_alias="t",
        type_map=write_config.data_type_map,
    ) > ex.convert(delta_load_value)
    logger.info(f"Start delta step 2, load updates by {delta_col.column_name}")
    upds_sql = _get_update_sql(
        cols=infos.col_infos,
        criterion=criterion,
        query=infos.from_("t"),
        write_config=write_config,
    )
    _load_updates_to_delta(
        logger,
        reader,
        sql=upds_sql,
        delta_path=delta_path,
        delta_name="delta_1",
        write_config=write_config,
    )
    if not simple:
        assert old_pk_version is not None
        _handle_additional_updates(
            infos=infos,
            old_pk_version=old_pk_version,
        )
        reader.local_register_update_view(delta_path, _temp_table(infos.table_or_query))

        logger.info("Start delta step 3.5, write meta for next delta load")

        write_latest_pk(
            reader, destination, infos.pk_cols, delta_col, write_config=write_config
        )

        logger.info("Start delta step 4.5, write deletes")
        do_deletes(
            reader=infos.source,
            destination=infos.destination,
            cols=infos.col_infos,
            pk_cols=infos.pk_cols,
            old_pk_version=old_pk_version,
            write_config=infos.write_config,
        )
        logger.info("Done delta load")
    else:
        _write_delta2(infos, [], mode="overwrite")  # just to create the delta_2 table
        pk_ts = destination / "delta_load" / DBDeltaPathConfigs.PRIMARY_KEYS_TS
        if pk_ts.exists():
            pk_ts.remove(True)

        write_latest_pk(
            reader,
            destination,
            infos.pk_cols,
            delta_col,
            write_config=write_config,
            merge_delta=True,
        )
        reader.local_register_update_view(
            destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION,
            DBDeltaPathConfigs.LATEST_PK_VERSION,
        )
        target_count = reader.local_execute_sql_to_py(
            sg.from_(DBDeltaPathConfigs.LATEST_PK_VERSION).select(
                ex.Count(this=ex.Star()).as_("cnt")
            )
        )[0]["cnt"]
        if source_count != target_count:
            logger.warning(
                f"Source and target count do not match. Source: {source_count}, Target: {target_count}. { 'Do a normal delta' if simple_check else ''}"
            )
            if simple_check:
                do_delta_load(infos, simple=False)


def do_append_inserts_load(infos: WriteConfigAndInfos):
    logger = infos.logger
    write_config = infos.write_config
    assert infos.delta_col is not None, "must have a delta col"
    logger.info(
        f"Start Append Only Load with Delta Column {infos.delta_col.column_name}"
    )
    delta_path = infos.destination / "delta"
    delta_load_value = _get_latest_delta_value(
        infos.source, delta_path, infos.delta_col, infos.write_config
    )

    criterion = (
        _source_convert(
            infos.delta_col.column_name,
            infos.delta_col.data_type,
            table_alias="t",
            type_map=write_config.data_type_map,
        )
        > ex.convert(delta_load_value)
        if delta_load_value
        else None
    )
    logger.info(" Start delta step 2, load updates by timestamp")
    _load_updates_to_delta(
        logger,
        infos.source,
        sql=_get_update_sql(
            cols=infos.col_infos,
            criterion=criterion,
            query=infos.from_("t"),
            write_config=write_config,
        ),
        delta_path=delta_path,
        delta_name="delta_1",
        write_config=write_config,
    )

    logger.info("Done Append only load")


def _get_latest_delta_value(
    reader: DataSourceReader,
    delta_path: Destination,
    delta_col: InformationSchemaColInfo,
    write_config: WriteConfig,
):
    tmp_view_name = "temp_" + str(abs(hash(str(delta_path))))
    reader.local_register_update_view(delta_path, tmp_view_name)
    return reader.local_execute_sql_to_py(
        sg.from_(ex.to_identifier(tmp_view_name)).select(
            ex.func("MAX", ex.column(write_config.get_target_name(delta_col))).as_(
                "max_ts"
            )
        )
    )[0]["max_ts"]


def do_deletes(
    reader: DataSourceReader,
    destination: Destination,
    # delta_table: DeltaTable,
    cols: Sequence[InformationSchemaColInfo],
    pk_cols: Sequence[InformationSchemaColInfo],
    old_pk_version: int,
    write_config: WriteConfig,
):
    reader.local_register_update_view(
        destination / f"delta_load/{ DBDeltaPathConfigs.LATEST_PK_VERSION}",
        DBDeltaPathConfigs.LATEST_PK_VERSION,
    )
    LAST_PK_VERSION = "LAST_PK_VERSION"
    reader.local_register_update_view(
        destination / f"delta_load/{ DBDeltaPathConfigs.LATEST_PK_VERSION}",
        LAST_PK_VERSION,
        version=old_pk_version,
    )
    delete_query = ex.except_(
        left=ex.select(
            *_get_cols_select(
                pk_cols,
                table_alias="lpk",
                system="target",
                get_target_name=write_config.get_target_name,
            )
        ).from_(table_from_tuple(LAST_PK_VERSION, alias="lpk")),
        right=ex.select(
            *_get_cols_select(
                pk_cols,
                table_alias="cpk",
                system="target",
                get_target_name=write_config.get_target_name,
            )
        ).from_(table_from_tuple(DBDeltaPathConfigs.LATEST_PK_VERSION, alias="cpk")),
    )

    non_pk_cols = [c for c in cols if c not in pk_cols]
    non_pk_select = [
        ex.Null().as_(write_config.get_target_name(c), quoted=True) for c in non_pk_cols
    ]
    deletes_with_schema = union(
        [
            ex.select(
                *_get_cols_select(
                    pk_cols,
                    table_alias="d1",
                    system="target",
                    get_target_name=write_config.get_target_name,
                )
            )
            .select(
                *_get_cols_select(
                    non_pk_cols,
                    table_alias="d1",
                    system="target",
                    get_target_name=write_config.get_target_name,
                ),
                append=True,
            )
            .select(
                ex.AtTimeZone(
                    this=ex.CurrentTimestamp(),
                    zone=ex.Literal(this="UTC", is_string=True),
                ).as_(VALID_FROM_COL_NAME),
                ex.convert(True).as_(IS_DELETED_COL_NAME),
                ex.convert(False).as_(IS_FULL_LOAD_COL_NAME),
            )
            .from_(table_from_tuple("delta_1", alias="d1"))
            .where("1=0"),  # only used to get correct datatypes
            ex.select(
                ex.Column(this=ex.Star(), table=ex.Identifier(this="d", quoted=False))
            )
            .select(*non_pk_select, append=True)
            .select(
                ex.AtTimeZone(
                    this=ex.CurrentTimestamp(),
                    zone=ex.Literal(this="UTC", is_string=True),
                ).as_(VALID_FROM_COL_NAME),
                append=True,
            )
            .select(ex.convert(True).as_(IS_DELETED_COL_NAME), append=True)
            .select(ex.convert(False).as_(IS_FULL_LOAD_COL_NAME), append=True)
            .from_(table_from_tuple("deletes", alias="d")),
        ],
        distinct=False,
    ).with_("deletes", as_=delete_query)
    reader.local_register_view(deletes_with_schema, "deletes_with_schema")
    has_deletes = (
        reader.local_execute_sql_to_py(count_limit_one("deletes_with_schema"))[0]["cnt"]
        > 0
    )
    if has_deletes:
        reader.local_execute_sql_to_delta(
            sg.from_("deletes_with_schema").select("*"),
            destination / "delta",
            mode="append",
        )


def _retrieve_primary_key_data(
    infos: WriteConfigAndInfos,
):
    pk_ts_col_select = infos.from_("t").select(
        *_get_cols_select(
            is_full=None,
            is_deleted=None,
            cols=concat_seq(
                infos.pk_cols, [infos.delta_col] if infos.delta_col else []
            ),
            with_valid_from=False,
            data_type_map=infos.write_config.data_type_map,
            system="source",
            get_target_name=infos.write_config.get_target_name,
        )
    )
    pk_ts_reader_sql = pk_ts_col_select.sql(infos.write_config.dialect)

    pk_path = infos.destination / f"delta_load/{DBDeltaPathConfigs.PRIMARY_KEYS_TS}"
    infos.logger.info("Retrieve all PK/TS", sql=pk_ts_reader_sql)
    infos.source.source_write_sql_to_delta(
        sql=pk_ts_reader_sql, delta_path=pk_path, mode="overwrite"
    )
    return pk_path


T = TypeVar("T")


def _list_to_chunks(input: Iterable[T], chunk_size: int):
    chunk: list[T] = list()
    for item in input:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = list()
    if len(chunk) > 0:
        yield chunk


def _write_delta2(
    infos: WriteConfigAndInfos, data: list[dict], mode: Literal["overwrite", "append"]
):
    write_config = infos.write_config
    from .query import sql_quote_value

    def _collate(c: InformationSchemaColInfo):
        if is_string_type(c.data_type):
            return "COLLATE Latin1_General_100_BIN "
        return ""

    delta_2_path = infos.destination / "delta_load" / DBDeltaPathConfigs.DELTA_2_NAME

    def full_sql(js: str):
        col_defs = ", ".join(
            [
                f"p{i} {p.data_type.sql(infos.write_config.dialect)}"
                for i, p in enumerate(infos.pk_cols)
            ]
        )

        selects = list(
            _get_cols_select(
                infos.col_infos,
                is_full=False,
                is_deleted=False,
                with_valid_from=True,
                table_alias="t",
                data_type_map=write_config.data_type_map,
                system="source",
                get_target_name=write_config.get_target_name,
            )
        )
        sql = infos.from_("t").select(*selects).sql(write_config.dialect)
        pk_map = ", ".join(
            [
                "p" + str(i) + " as " + sql_quote_name(write_config.get_target_name(c))
                for i, c in enumerate(infos.pk_cols)
            ]
        )
        return f"""{sql}
        inner join (SELECT {pk_map} FROM OPENJSON({sql_quote_value(js)}) with ({col_defs}) ) ttt
             on {' AND '.join([f't.{sql_quote_name(c.column_name)} {_collate(c)} = ttt.{sql_quote_name(write_config.get_target_name(c))}' for c in infos.pk_cols])}
        """

    if mode == "overwrite":
        infos.logger.info(
            "execute sql",
            load="delta",
            sub_load="delta_additional",
            sql=full_sql(f"[/* {len(data)} entries */]"),
        )
    sql = full_sql(json.dumps(data))
    if (
        len(sql) > 7000
    ):  ## oops, spark will not like this (actually the limit is 8000, but spark might use something on it's own)
        ch_split = len(data) // 2
        chunk_1 = data[:ch_split]
        chunk_2 = data[ch_split:]
        infos.source.source_write_sql_to_delta(
            full_sql(json.dumps(chunk_1)),
            delta_2_path,
            mode=mode,
        )
        infos.source.source_write_sql_to_delta(
            full_sql(json.dumps(chunk_2)),
            delta_2_path,
            mode="append",
        )
    else:
        infos.source.source_write_sql_to_delta(
            sql,
            delta_2_path,
            mode=mode,
        )


def _handle_additional_updates(
    infos: WriteConfigAndInfos,
    old_pk_version: int,
):
    """Handles updates that are not logical by their timestamp. This can happen on a restore from backup, for example."""
    folder = infos.destination
    delta_col = infos.delta_col
    pk_cols = infos.pk_cols
    cols = infos.col_infos
    reader = infos.source
    logger = infos.logger
    write_config = infos.write_config
    assert delta_col is not None, "Need a delta column"
    pk_ds_cols = concat_seq(pk_cols, [delta_col])
    reader.local_register_update_view(
        folder / f"delta_load/{ DBDeltaPathConfigs.PRIMARY_KEYS_TS}",
        DBDeltaPathConfigs.PRIMARY_KEYS_TS,
    )
    LAST_PK_VERSION = "LAST_PK_VERSION"
    reader.local_register_update_view(
        folder / f"delta_load/{ DBDeltaPathConfigs.LATEST_PK_VERSION}",
        LAST_PK_VERSION,
        version=old_pk_version,
    )

    reader.local_register_view(
        ex.except_(
            left=ex.select(
                *_get_cols_select(
                    cols=pk_ds_cols,
                    table_alias="pk",
                    system="target",
                    get_target_name=write_config.get_target_name,
                )
            ).from_(ex.table_(DBDeltaPathConfigs.PRIMARY_KEYS_TS, alias="pk")),
            right=ex.select(
                *_get_cols_select(
                    cols=pk_ds_cols,
                    table_alias="lpk",
                    system="target",
                    get_target_name=write_config.get_target_name,
                )
            ).from_(table_from_tuple(LAST_PK_VERSION, alias="lpk")),
        ),
        "additional_updates",
    )

    sql_query = ex.except_(
        left=ex.select(
            *_get_cols_select(
                cols=pk_cols,
                table_alias="au",
                system="target",
                get_target_name=write_config.get_target_name,
            )
        ).from_(ex.table_("additional_updates", alias="au")),
        right=ex.select(
            *_get_cols_select(
                cols=pk_cols,
                table_alias="d1",
                system="target",
                get_target_name=write_config.get_target_name,
            )
        ).from_(table_from_tuple("delta_1", alias="d1")),
    )
    reader.local_register_view(sql_query, "real_additional_updates")
    update_count: int = reader.local_execute_sql_to_py(
        sg.from_("real_additional_updates").select(ex.Count(this=ex.Star()).as_("cnt"))
    )[0]["cnt"]

    jsd = reader.local_execute_sql_to_py(
        sg.from_("real_additional_updates").select(
            *[
                ex.column(write_config.get_target_name(c)).as_(
                    "p" + str(i), quoted=False
                )
                for i, c in enumerate(pk_cols)
            ]
        )
    )

    if update_count == 0:
        _write_delta2(infos, [], mode="overwrite")
    elif (
        (update_count > 1000) or write_config.no_complex_entries_load
    ):  # many updates. get the smallest timestamp and do "normal" delta, even if there are too many records then
        _write_delta2(infos, [], mode="overwrite")  # still need to create delta_2_path
        logger.warning(
            f"Start delta step 3, load {update_count} strange updates via normal delta load"
        )
        delta_load_value = reader.local_execute_sql_to_py(
            ex.select(
                ex.func(
                    "MIN",
                    ex.column(write_config.get_target_name(delta_col), quoted=True),
                ).as_("min_ts")
            ).from_(ex.table_("additional_updates", alias="rau"))
        )[0]["min_ts"]
        criterion = _source_convert(
            delta_col.column_name,
            delta_col.data_type,
            table_alias="t",
            type_map=write_config.data_type_map,
        ) > ex.convert(delta_load_value)
        logger.info("Start delta step 2, load updates by timestamp")
        upds_sql = _get_update_sql(
            cols=cols,
            criterion=criterion,
            query=infos.from_("t"),
            write_config=write_config,
        )
        logger.info(
            "execute sql", load="delta", sub_load="delta_1_additional", sql=upds_sql
        )
        _load_updates_to_delta(
            logger,
            reader,
            sql=upds_sql,
            delta_path=infos.destination / "delta",
            delta_name="delta_1",
            write_config=write_config,
        )
    else:
        # we don't want to overshoot 8000 chars here because of spark. we estimate how much space in json a record of pk's will take

        char_size_pks = sum(
            [
                5
                + (
                    10
                    if p.data_type
                    in ["bit", "int", "bigint", "tinyint", "bool", "smallint"]
                    else 40
                )
                for p in pk_cols
            ]
        )
        batch_size = max(10, int(7000 / char_size_pks))

        logger.warning(
            "Start delta step 3, load {update_count} strange updates via batches of size {batch_size}"
        )
        first = True
        for chunk in _list_to_chunks(jsd, batch_size):
            _write_delta2(infos, chunk, mode="overwrite" if first else "append")
            first = False
        reader.local_register_update_view(
            infos.destination / "delta_load" / DBDeltaPathConfigs.DELTA_2_NAME,
            "delta_2",
        )
        reader.local_execute_sql_to_delta(
            sg.from_("delta_2").select(ex.Star()),
            infos.destination / "delta",
            mode="append",
        )


def _get_update_sql(
    cols: Sequence[InformationSchemaColInfo],
    criterion: Union[Sequence[ex.Expression], ex.Expression, None],
    query: ex.Select,
    write_config: WriteConfig,
):
    if isinstance(criterion, ex.Expression):
        criterion = [criterion]
    delta_sql = (
        query.select(
            *_get_cols_select(
                cols,
                is_full=False,
                is_deleted=False,
                with_valid_from=True,
                table_alias="t",
                data_type_map=write_config.data_type_map,
                system="source",
                get_target_name=write_config.get_target_name,
            )
        )
        .where(
            *(
                criterion
                if criterion is not None and not isinstance(criterion, str)
                else []
            ),
            dialect=write_config.dialect,
        )
        .sql(write_config.dialect)
    )
    return delta_sql


def _load_updates_to_delta(
    logger: DeltaLogger,
    reader: DataSourceReader,
    delta_path: Destination,
    sql: Union[str, ex.Query],
    delta_name: str,
    write_config: WriteConfig,
):
    if isinstance(sql, ex.Query):
        sql = sql.sql(write_config.dialect)

    delta_name_path = delta_path.parent / f"delta_load/{delta_name}"
    logger.info("Executing sql", load="delta", sub_load=delta_name, sql=sql)
    reader.source_write_sql_to_delta(sql, delta_name_path, mode="overwrite")
    reader.local_register_update_view(delta_name_path, delta_name)
    count = reader.local_execute_sql_to_py(count_limit_one(delta_name))[0]["cnt"]
    if count == 0:
        return
    reader.local_execute_sql_to_delta(
        sg.from_(delta_name).select(ex.Star()), delta_path, mode="append"
    )


def do_full_load(infos: WriteConfigAndInfos, mode: Literal["overwrite", "append"]):
    logger = infos.logger
    write_config = infos.write_config
    delta_path = infos.destination / "delta"
    reader = infos.source
    logger.info("Start Full Load")
    sql = (
        infos.from_("t")
        .select(
            *_get_cols_select(
                is_deleted=False,
                is_full=True,
                cols=infos.col_infos,
                with_valid_from=True,
                data_type_map=write_config.data_type_map,
                system="source",
                get_target_name=write_config.get_target_name,
            )
        )
        .sql(write_config.dialect)
    )
    logger.info("executing sql", sql=sql, load="full")
    reader.source_write_sql_to_delta(sql, delta_path, mode=mode)
    if infos.delta_col is None:
        logger.info("Full Load done")
        return
    logger.info(" Full Load done, write meta for delta load")

    reader.local_register_update_view(delta_path, _temp_table(infos.table_or_query))
    (delta_path.parent / "delta_load").mkdir()
    query = sg.from_(ex.to_identifier(_temp_table(infos.table_or_query))).select(
        *(
            [
                ex.column(write_config.get_target_name(pk), quoted=True)
                for pk in infos.pk_cols
            ]
            + (
                [ex.column(write_config.get_target_name(infos.delta_col), quoted=True)]
                if infos.delta_col
                else []
            )
        )
    )
    reader.local_execute_sql_to_delta(
        query,
        delta_path.parent / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION,
        mode="overwrite",
    )
