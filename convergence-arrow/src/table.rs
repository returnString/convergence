use arrow::array::{Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use convergence::protocol::{DataTypeOid, FieldDescription, FormatCode, RowDescription};
use convergence::protocol_ext::DataRowBatch;

macro_rules! array_cast {
	($arrtype: ident, $arr: expr) => {
		$arr.as_any().downcast_ref::<$arrtype>().expect("array cast failed")
	};
}

macro_rules! array_val {
	($arrtype: ident, $arr: expr, $idx: expr, $value_func: ident) => {
		array_cast!($arrtype, $arr).$value_func($idx)
	};
	($arrtype: ident, $arr: expr, $idx: expr) => {
		array_val!($arrtype, $arr, $idx, value)
	};
}

pub fn record_batch_to_rows(arrow_batch: &RecordBatch, pg_batch: &mut DataRowBatch) {
	for row_idx in 0..arrow_batch.num_rows() {
		let mut row = pg_batch.create_row();
		for col_idx in 0..arrow_batch.num_columns() {
			let col = arrow_batch.column(col_idx);
			if col.is_null(row_idx) {
				row.null();
			} else {
				match col.data_type() {
					DataType::Int32 => row.i32(array_val!(Int32Array, col, row_idx)),
					DataType::Int64 => row.i64(array_val!(Int64Array, col, row_idx)),
					DataType::UInt32 => row.i32(array_val!(UInt32Array, col, row_idx) as i32),
					DataType::UInt64 => row.i64(array_val!(UInt64Array, col, row_idx) as i64),
					DataType::Utf8 => row.string(array_val!(StringArray, col, row_idx)),
					_ => unimplemented!(),
				};
			}
		}
	}
}

pub fn data_type_to_oid(ty: &DataType) -> DataTypeOid {
	match ty {
		DataType::Int32 => DataTypeOid::Int4,
		DataType::Int64 => DataTypeOid::Int8,
		// TODO: need to figure out a sensible mapping here
		DataType::UInt32 => DataTypeOid::Int4,
		DataType::UInt64 => DataTypeOid::Int8,
		DataType::Utf8 => DataTypeOid::Text,
		other => unimplemented!("arrow to pg conversion not implemented: {}", other),
	}
}

pub fn schema_to_row_desc(schema: &Schema, format_code: FormatCode) -> RowDescription {
	let fields = schema
		.fields()
		.iter()
		.map(|f| FieldDescription {
			name: f.name().clone(),
			data_type: data_type_to_oid(f.data_type()),
		})
		.collect();

	RowDescription { fields, format_code }
}
