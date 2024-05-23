from .write_init import WriteConfigAndInfos, DBDeltaPathConfigs
from .write_utils.restore_pk import create_last_pk_version_view
import sqlglot as sg
import sqlglot.expressions as ex


def check_latest_pk(infos: WriteConfigAndInfos, raise_if_not_consistent=True):
    if not infos.delta_col or not infos.pk_cols:
        raise ValueError("Primary keys and delta column must be defined")
    postfix = "_" + str(abs(hash(str(infos.destination))))
    lpk_view = "lastest_pk_" + postfix
    infos.source.local_register_update_view(
        infos.destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION,
        lpk_view,
    )
    _, view_name, success = create_last_pk_version_view(
        infos, view_prefix="v_" + postfix
    )
    d_cols = list(infos.pk_cols) + [infos.delta_col]
    col_names = [infos.write_config.get_target_name(p) for p in d_cols]
    assert success
    assert view_name is not None
    query1 = sg.except_(
        sg.from_(ex.table_(lpk_view, alias="lpk"))
        .select(*[ex.column(c, "lpk", quoted=True) for c in col_names])
        .select(ex.convert("added in persisted data"), append=True),
        sg.from_(ex.table_(view_name, alias="rs"))
        .select(*[ex.column(c, "rs", quoted=True) for c in col_names])
        .select(ex.convert("added in persisted data"), append=True),
    )
    query2 = sg.except_(
        sg.from_(ex.table_(view_name, alias="rs"))
        .select(*[ex.column(c, "rs", quoted=True) for c in col_names])
        .select(ex.convert("missing in persisted data"), append=True),
        sg.from_(ex.table_(lpk_view, alias="lpk"))
        .select(*[ex.column(c, "lpk", quoted=True) for c in col_names])
        .select(ex.convert("missing in persisted data"), append=True),
    )
    result = infos.source.local_execute_sql_to_py(
        ex.union(query1, query2, distinct=False)
    )
    if result:
        print(result)
        if raise_if_not_consistent:
            raise ValueError("Primary keys are not consistent")
    return result
