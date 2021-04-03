use crate::protocol::{ConnectionCodec, FormatCode, ProtocolError, RowDescription};
use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;

pub struct DataRowBatch {
	format_code: FormatCode,
	num_cols: usize,
	num_rows: usize,
	data: BytesMut,
	row: BytesMut,
}

impl DataRowBatch {
	pub fn new(format_code: FormatCode, num_cols: usize) -> Self {
		Self {
			format_code,
			num_cols,
			num_rows: 0,
			data: BytesMut::new(),
			row: BytesMut::new(),
		}
	}

	pub fn from_row_desc(desc: &RowDescription) -> Self {
		Self::new(desc.format_code, desc.fields.len())
	}

	pub fn create_row(&mut self) -> DataRowWriter {
		self.num_rows += 1;
		DataRowWriter::new(self)
	}

	pub fn num_rows(&self) -> usize {
		self.num_rows
	}
}

macro_rules! primitive_write {
	($name: ident, $type: ident) => {
		pub fn $name(&mut self, val: $type) {
			match self.parent.format_code {
				FormatCode::Text => self.write_value(&val.to_string().into_bytes()),
				FormatCode::Binary => self.write_value(&val.to_be_bytes()),
			};
		}
	};
}

pub struct DataRowWriter<'a> {
	current_col: usize,
	parent: &'a mut DataRowBatch,
}

impl<'a> DataRowWriter<'a> {
	pub fn new(parent: &'a mut DataRowBatch) -> Self {
		parent.row.put_i16(parent.num_cols as i16);
		Self { current_col: 0, parent }
	}

	pub fn write_value(&mut self, data: &[u8]) {
		self.current_col += 1;
		self.parent.row.put_i32(data.len() as i32);
		self.parent.row.put_slice(data);
	}

	pub fn write_null(&mut self) {
		self.parent.row.put_i32(-1);
	}

	pub fn write_string(&mut self, val: &str) {
		self.write_value(val.as_bytes());
	}

	primitive_write!(write_int2, i16);
	primitive_write!(write_int4, i32);
	primitive_write!(write_int8, i64);
	primitive_write!(write_float4, f32);
	primitive_write!(write_float8, f64);
}

impl<'a> Drop for DataRowWriter<'a> {
	fn drop(&mut self) {
		assert_eq!(
			self.parent.num_cols, self.current_col,
			"dropped a row writer with an invalid number of columns"
		);

		self.parent.data.put_u8(b'D');
		self.parent.data.put_i32((self.parent.row.len() + 4) as i32);
		self.parent.data.extend(self.parent.row.split());
	}
}

impl Encoder<DataRowBatch> for ConnectionCodec {
	type Error = ProtocolError;

	fn encode(&mut self, item: DataRowBatch, dst: &mut BytesMut) -> Result<(), Self::Error> {
		dst.extend(item.data);
		Ok(())
	}
}
