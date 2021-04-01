use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use convergence::protocol::{DataRow, DataTypeOid, FieldDescription, FormatCode, RowDescription};

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

macro_rules! pg_bytes_from_arrow {
	($arrtype: ident, $arr: expr, $idx: expr, $format: expr) => {
		pg_bytes_from_arrow!($arrtype, $arr, $idx, $format, to_be_bytes, to_vec)
	};
	($arrtype: ident, $arr: expr, $idx: expr, $format: expr, $raw_getter: ident, $post_getter: ident) => {{
		let val = array_val!($arrtype, $arr, $idx);

		match $format {
			FormatCode::Text => val.to_string().into_bytes(),
			FormatCode::Binary => val.$raw_getter().$post_getter(),
		}
	}};
}

pub fn record_batch_to_rows(batch: &RecordBatch, format_code: FormatCode) -> Vec<DataRow> {
	let mut ret = Vec::new();

	for row_idx in 0..batch.num_rows() {
		let mut row_values = Vec::new();
		for col_idx in 0..batch.num_columns() {
			let col = batch.column(col_idx);
			if col.is_null(row_idx) {
				row_values.push(None);
			} else {
				let bytes = match col.data_type() {
					DataType::Int32 => pg_bytes_from_arrow!(Int32Array, col, row_idx, format_code),
					DataType::Int64 => pg_bytes_from_arrow!(Int64Array, col, row_idx, format_code),
					DataType::Float64 => pg_bytes_from_arrow!(Float64Array, col, row_idx, format_code),
					DataType::Utf8 => pg_bytes_from_arrow!(StringArray, col, row_idx, format_code, as_bytes, to_owned),
					_ => unimplemented!(),
				};

				row_values.push(Some(bytes));
			}
		}

		ret.push(DataRow { values: row_values });
	}

	ret
}

pub fn data_type_to_oid(ty: &DataType) -> DataTypeOid {
	match ty {
		DataType::Int32 => DataTypeOid::Int4,
		DataType::Utf8 => DataTypeOid::Text,
		_ => unimplemented!(),
	}
}

pub fn record_batch_to_row_desc(batch: &RecordBatch, format_code: FormatCode) -> RowDescription {
	let fields = batch
		.schema()
		.fields()
		.iter()
		.map(|f| FieldDescription {
			name: f.name().clone(),
			data_type: data_type_to_oid(f.data_type()),
		})
		.collect();

	RowDescription { fields, format_code }
}
