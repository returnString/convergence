use arrow::array::{ArrayRef, Float64Builder, StringBuilder};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use rusoto_dynamodb::AttributeValue;
use std::collections::HashMap;
use std::sync::Arc;

pub fn items_to_record_batch(
	items: &[HashMap<String, AttributeValue>],
	schema: SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
	let mut columns = Vec::new();
	for field in schema.fields() {
		let array: ArrayRef = match field.data_type() {
			DataType::Utf8 => {
				let mut builder = StringBuilder::new(items.len());
				for item in items {
					let attr = item.get(field.name());
					match attr.and_then(|v| v.s.as_ref()) {
						Some(value) => builder.append_value(value)?,
						None => builder.append_null()?,
					}
				}
				Arc::new(builder.finish())
			}
			DataType::Float64 => {
				let mut builder = Float64Builder::new(items.len());
				for item in items {
					let attr = item.get(field.name());
					match attr.and_then(|v| v.n.as_ref()) {
						Some(value) => builder.append_value(
							value
								.parse::<f64>()
								.map_err(|err| DataFusionError::Execution(err.to_string()))?,
						)?,
						None => builder.append_null()?,
					}
				}
				Arc::new(builder.finish())
			}
			other => {
				return Err(DataFusionError::Execution(format!(
					"unsupported datatype for dynamodb item translation: {}",
					other
				)))
			}
		};

		columns.push(array);
	}

	Ok(RecordBatch::try_new(schema, columns)?)
}
