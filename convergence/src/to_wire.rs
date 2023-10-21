use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

const NULL: i32 = -1;

struct Null {}

pub trait Writer {
	fn write<T>(&mut self, val: T)
	where
		T: ToWire;
}

pub trait ToWire {
	fn to_binary(&self) -> Vec<u8>;

	fn to_text(&self) -> Vec<u8>;
}

impl<T: ToWire> ToWire for Option<T> {
	fn to_binary(&self) -> Vec<u8> {
		match self {
			Some(ref val) => val.to_binary(),
			None => NULL.to_binary(),
		}
	}
	fn to_text(&self) -> Vec<u8> {
		match self {
			Some(ref val) => val.to_text(),
			None => NULL.to_binary(),
		}
	}
}

impl ToWire for bool {
	fn to_binary(&self) -> Vec<u8> {
		(*self as u8).to_be_bytes().into()
	}
	fn to_text(&self) -> Vec<u8> {
		if *self { "t" } else { "f" }.as_bytes().into()
	}
}

fn pg_date_epoch() -> NaiveDate {
	NaiveDate::from_ymd_opt(2000, 1, 1).expect("failed to create pg date epoch")
}

fn pg_timestamp_epoch() -> NaiveDateTime {
	pg_date_epoch()
		.and_hms_opt(0, 0, 0)
		.expect("failed to create pg timestamp epoch")
}

impl ToWire for NaiveDate {
	fn to_binary(&self) -> Vec<u8> {
		let d: i32 = self.signed_duration_since(pg_date_epoch()).num_days() as i32;
		d.to_binary()
	}
	fn to_text(&self) -> Vec<u8> {
		self.to_string().as_bytes().into()
	}
}

impl ToWire for NaiveDateTime {
	fn to_binary(&self) -> Vec<u8> {
		let dt: i64 = self
			.signed_duration_since(pg_timestamp_epoch())
			.num_microseconds()
			.unwrap();
		dt.to_binary()
	}
	fn to_text(&self) -> Vec<u8> {
		self.to_string().as_bytes().into()
	}
}

impl ToWire for NaiveTime {
	fn to_binary(&self) -> Vec<u8> {
		let delta = self.signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
		let t = delta.num_microseconds().unwrap_or(0);
		t.to_binary()
	}
	fn to_text(&self) -> Vec<u8> {
		self.to_string().as_bytes().into()
	}
}

macro_rules! to_wire {
	($type: ident) => {
		#[allow(missing_docs)]
		impl ToWire for $type {
			fn to_binary(&self) -> Vec<u8> {
				self.to_be_bytes().into()
			}
			fn to_text(&self) -> Vec<u8> {
				self.to_string().as_bytes().into()
			}
		}
	};
}

to_wire!(i8);
to_wire!(i16);
to_wire!(i32);
to_wire!(i64);
to_wire!(f32);
to_wire!(f64);

#[cfg(test)]
mod tests {
	use bytes::{BufMut, BytesMut};
	use rand::Rng;
	use std::{convert::TryInto, mem};

	use super::{ToWire, Writer};

	struct TestWriter {
		pub buf: BytesMut,
	}

	impl Writer for TestWriter {
		fn write<T>(&mut self, val: T)
		where
			T: ToWire,
		{
			self.buf.put_slice(&val.to_binary());
		}
	}

	macro_rules! test_to_wire {
		($name: ident, $type: ident) => {
			#[test]
			pub fn $name() {
				const LEN: usize = mem::size_of::<$type>();

				let min: $type = 0 as $type;
				let max: $type = $type::MAX;

				let mut rng = rand::thread_rng();
				let expected: $type = rng.gen_range(min..max);

				let val: $type = expected;

				let mut w = TestWriter { buf: BytesMut::new() };
				w.write(val);

				let data: [u8; LEN] = w.buf[..LEN].try_into().expect("Expected $type");
				let out = $type::from_be_bytes(data);

                assert_eq!(expected, out);

                // Option<T>
                let val: Option<$type> = Some(expected);

				let mut w = TestWriter { buf: BytesMut::new() };
				w.write(val);

				let data: [u8; LEN] = w.buf[..LEN].try_into().expect("Expected $type");
				let out = $type::from_be_bytes(data);

				assert_eq!(expected, out);
			}
		};
	}

	macro_rules! test_to_wire_null {
		($name: ident, $type: ident) => {
			#[tokio::test]
			pub async fn $name() {
                // Option<T>
                let val: Option<$type> = None;

				let mut w = TestWriter { buf: BytesMut::new() };
				w.write(val);

				let data: [u8; 4] = w.buf[..4].try_into().expect("Expected $type");
				let out = i32::from_be_bytes(data);

                let expected: i32 = -1;
				assert_eq!(expected, out);
			}
		};
	}

	test_to_wire!(test_to_wire_i16, i16);
	test_to_wire!(test_to_wire_i32, i32);
	test_to_wire!(test_to_wire_i64, i64);
	test_to_wire!(test_to_wire_f32, f32);
	test_to_wire!(test_to_wire_f64, f64);

	test_to_wire_null!(test_to_wire_null_i16, i16);
	test_to_wire_null!(test_to_wire_null_i32, i32);
	test_to_wire_null!(test_to_wire_null_i64, i64);
	test_to_wire_null!(test_to_wire_null_f32, f32);
	test_to_wire_null!(test_to_wire_null_f64, f64);
}
