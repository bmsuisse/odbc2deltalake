from typing import Union, Mapping, Sequence, TYPE_CHECKING
import sqlglot.expressions as ex

from odbc2deltalake.metadata import InformationSchemaColInfo
from odbc2deltalake.reader.reader import DeltaOps

if TYPE_CHECKING:
    import pyarrow as pa
    from deltalake.schema import PrimitiveType, MapType, ArrayType, StructType
    from deltalake import DeltaTable


def _all_nullable(schema: "pa.Schema") -> "pa.Schema":
    sc = schema
    for i, n in enumerate(schema.names):
        f = schema.field(n)
        if not f.nullable:
            sc = sc.set(i, f.with_nullable(True))
    return sc


def _get_type(tp: "pa.DataType"):
    import pyarrow.types as pat

    if pat.is_string(tp):
        return ex.DataType.Type.NVARCHAR
    if pat.is_boolean(tp):
        return ex.DataType.Type.BIT
    if pat.is_int8(tp):
        return ex.DataType.Type.TINYINT
    if pat.is_int16(tp):
        return ex.DataType.Type.SMALLINT
    if pat.is_int32(tp):
        return ex.DataType.Type.INT
    if pat.is_int64(tp):
        return ex.DataType.Type.BIGINT
    if pat.is_float32(tp):
        return ex.DataType.Type.FLOAT
    if pat.is_float64(tp):
        return ex.DataType.Type.DOUBLE
    if pat.is_date32(tp):
        return ex.DataType.Type.DATE
    if pat.is_date64(tp):
        return ex.DataType.Type.DATETIME
    if pat.is_timestamp(tp):
        return ex.DataType.Type.DATETIME
    if pat.is_time32(tp):
        return ex.DataType.Type.TIME
    if pat.is_time64(tp):
        return ex.DataType.Type.TIME
    if pat.is_decimal(tp):
        return ex.DataType.Type.DECIMAL
    if pat.is_binary(tp):
        return ex.DataType.Type.VARBINARY
    if pat.is_fixed_size_binary(tp):
        return ex.DataType.build(f"binary({tp.byte_width})", dialect="tsql")
    raise ValueError(f"Type {tp} not supported")


def _build_type(t: Union[ex.DataType, ex.DataType.Type]):
    if isinstance(t, ex.DataType):
        return t
    return ex.DataType(this=t)


def _delta_to_sq_type(
    delta_type: "PrimitiveType | MapType | ArrayType | StructType",
) -> ex.DataType:
    from deltalake.schema import PrimitiveType, MapType, ArrayType, StructType

    if isinstance(delta_type, PrimitiveType):
        type_map = {
            "string": ex.DataType.Type.NVARCHAR,
            "long": ex.DataType.Type.BIGINT,
            "integer": ex.DataType.Type.INT,
            "short": ex.DataType.Type.SMALLINT,
            "byte": ex.DataType.Type.TINYINT,
            "float": ex.DataType.Type.FLOAT,
            "double": ex.DataType.Type.DOUBLE,
            "boolean": ex.DataType.Type.BIT,
            "binary": ex.DataType.Type.VARBINARY,
            "date": ex.DataType.Type.DATE,
            "timestamp": ex.DataType.Type.DATETIME,
            "timestampNtz": ex.DataType.Type.DATETIME,
        }
        if delta_type.type in type_map:
            return ex.DataType.build(type_map[delta_type.type], dialect="spark")
        else:
            return ex.DataType.build(delta_type.type, dialect="spark")
    elif isinstance(delta_type, MapType):
        key_type = _delta_to_sq_type(delta_type.key_type)
        value_type = _delta_to_sq_type(delta_type.value_type)

        return ex.DataType(
            this=ex.DataType.Type.MAP, expressions=[key_type, value_type], nested=True
        )
    elif isinstance(delta_type, ArrayType):
        item_type = _delta_to_sq_type(delta_type.element_type)
        return ex.DataType(
            this=ex.DataType.Type.ARRAY, expressions=[item_type], nested=True
        )
    elif isinstance(delta_type, StructType):
        fields = [
            ex.DataType(
                this=ex.DataType.Type.STRUCT,
                expressions=[
                    ex.ColumnDef(
                        this=ex.to_identifier(f.name),
                        kind=_delta_to_sq_type(f.type),
                    ),
                ],
                nested=True,
            )
            for f in delta_type.fields
        ]
        return ex.DataType(
            this=ex.DataType.Type.STRUCT, expressions=fields, nested=True
        )
    else:
        raise ValueError(f"Unsupported Delta type: {type(delta_type)}")


class DeltaRSDeltaOps(DeltaOps):
    def __init__(self, delta_table: "DeltaTable"):
        self.delta_table = delta_table

    def version(self) -> int:
        return self.delta_table.version()

    def vacuum(self, retention_hours: Union[int, None] = None):
        self.delta_table.vacuum(retention_hours)

    def restore(self, target: int):
        self.delta_table.restore(target)

    def set_properties(self, props: dict[str, str]):
        self.delta_table.delete("1=0", custom_metadata=props)

    def get_property(self, key: str):
        return self.delta_table.metadata().configuration.get(key, None)

    def set_nullable(self, cols: Mapping[str, bool]):
        # not implemented properly for delta-rs
        for k, v in cols.items():
            if v:
                self.delta_table.alter.add_constraint({f"null_{k}": f"{k} IS NOT NULL"})
            else:
                self.delta_table.alter.drop_constraint(
                    f"null_{k}", raise_if_not_exists=False
                )

    def column_infos(self) -> "Sequence[InformationSchemaColInfo]":
        from odbc2deltalake.metadata import InformationSchemaColInfo

        fields = self.delta_table.schema().fields
        return [
            InformationSchemaColInfo(
                column_name=f.name,
                data_type_str=str(f.type),
                data_type=_delta_to_sq_type(f.type),
                is_nullable=f.nullable,
            )
            for f in fields
        ]

    def update_incremental(self):
        self.delta_table.update_incremental()
