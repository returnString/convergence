//! Contains types that represent the core Postgres wire protocol.

// this module requires a lot more work to document
// may want to build this automatically from Postgres docs if possible
#![allow(missing_docs)]

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::convert::TryFrom;
use std::fmt::Display;
use std::mem::size_of;
use std::{collections::HashMap, convert::TryInto};
use tokio_util::codec::{Decoder, Encoder};

macro_rules! data_types {
	($($name:ident = $oid:expr, $size: expr)*) => {
		#[derive(Debug, Copy, Clone)]
		/// Describes a Postgres data type.
		pub enum DataTypeOid {
			$(
				#[allow(missing_docs)]
				$name,
			)*
			/// A type which is not known to this crate.
			Unknown(u32),
		}

		impl DataTypeOid {
			/// Fetch the size in bytes for this data type.
			/// Variably-sized types return -1.
			pub fn size_bytes(&self) -> i16 {
				match self {
					$(
						Self::$name => $size,
					)*
					Self::Unknown(oid) => {
						tracing::warn!("Unknown data type OID {}", oid);
						-1 //Assume variable length
					}
				}
			}
		}

		impl From<u32> for DataTypeOid {
			fn from(value: u32) -> Self {
				match value {
					$(
						$oid => Self::$name,
					)*
					other => Self::Unknown(other),
				}
			}
		}

		impl From<DataTypeOid> for u32 {
			fn from(value: DataTypeOid) -> Self {
				match value {
					$(
						DataTypeOid::$name => $oid,
					)*
					DataTypeOid::Unknown(other) => other,
				}
			}
		}
	};
}

// For oid see:
// https://github.com/sfackler/rust-postgres/blob/master/postgres-types/src/type_gen.rs
data_types! {
	Unspecified = 0, 0
	Bool = 16, 1
	Bytea = 17, -1
	Char = 18, 1
	Name = 19, 64
	Int8 = 20, 8
	Int2 = 21, 2
	Int2Vector = 22, 2
	Int4 = 23, 4
	Regproc = 24, -1
	Text = 25, -1
	Oid = 26, -1
	Tid = 27, -1
	Xid = 28, -1
	Cid = 29, -1
	OidVector = 30, -1
	PgDdlCommand = 32, -1
	Json = 114, -1
	Xml = 142, -1
	XmlArray = 143, -1
	PgNodeTree = 194, -1
	JsonArray = 199, -1
	TableAmHandler = 269, -1
	Xid8Array = 271, -1
	IndexAmHandler = 325, -1
	Point = 600, -1
	Lseg = 601, -1
	Path = 602, -1
	Box = 603, -1
	Polygon = 604, -1
	Line = 628, -1
	LineArray = 629, -1
	Cidr = 650, -1
	CidrArray = 651, -1
	Float4 = 700, 4
	Float8 = 701, 8
	// Unknown = 705, -1
	Circle = 718, -1
	CircleArray = 719, -1
	Macaddr8 = 774, -1
	Macaddr8Array = 775, -1
	Money = 790, -1
	MoneyArray = 791, -1
	Macaddr = 829, -1
	Inet = 869, -1
	BoolArray = 1000, -1
	ByteaArray = 1001, -1
	CharArray = 1002, -1
	NameArray = 1003, -1
	Int2Array = 1005, -1
	Int2VectorArray = 1006, -1
	Int4Array = 1007, -1
	RegprocArray = 1008, -1
	TextArray = 1009, -1
	TidArray = 1010, -1
	XidArray = 1011, -1
	CidArray = 1012, -1
	OidVectorArray = 1013, -1
	BpcharArray = 1014, -1
	VarcharArray = 1015, -1
	Int8Array = 1016, -1
	PointArray = 1017, -1
	LsegArray = 1018, -1
	PathArray = 1019, -1
	BoxArray = 1020, -1
	Float4Array = 1021, -1
	Float8Array = 1022, -1
	PolygonArray = 1027, -1
	OidArray = 1028, -1
	Aclitem = 1033, -1
	AclitemArray = 1034, -1
	MacaddrArray = 1040, -1
	InetArray = 1041, -1
	Bpchar = 1042, -1
	Varchar = 1043, -1
	Date = 1082, 4
	Time = 1083, 8
	Timestamp = 1114, 8
	TimestampArray = 1115, -1
	DateArray = 1182, -1
	TimeArray = 1183, -1
	Timestamptz = 1184, 8
	TimestamptzArray = 1185, -1
	Interval = 1186, 16
	IntervalArray = 1187, -1
	NumericArray = 1231, -1
	CstringArray = 1263, -1
	Timetz = 1266, 12
	TimetzArray = 1270, -1
	Bit = 1560, 1
	BitArray = 1561, -1
	Varbit = 1562, -1
	VarbitArray = 1563, -1
	Numeric = 1700, -1
	Refcursor = 1790, -1
	RefcursorArray = 2201, -1
	Regprocedure = 2202, -1
	Regoper = 2203, -1
	Regoperator = 2204, -1
	Regclass = 2205, -1
	Regtype = 2206, -1
	RegprocedureArray = 2207, -1
	RegoperArray = 2208, -1
	RegoperatorArray = 2209, -1
	RegclassArray = 2210, -1
	RegtypeArray = 2211, -1
	Record = 2249, -1
	Cstring = 2275, -1
	Any = 2276, -1
	Anyarray = 2277, -1
	Void = 2278, -1
	Trigger = 2279, -1
	LanguageHandler = 2280, -1
	Internal = 2281, -1
	Anyelement = 2283, -1
	RecordArray = 2287, -1
	Anynonarray = 2776, -1
	TxidSnapshotArray = 2949, -1
	Uuid = 2950, 16
	UuidArray = 2951, -1
	TxidSnapshot = 2970, -1
	FdwHandler = 3115, -1
	PgLsn = 3220, -1
	PgLsnArray = 3221, -1
	TsmHandler = 3310, -1
	PgNdistinct = 3361, -1
	PgDependencies = 3402, -1
	Anyenum = 3500, -1
	TsVector = 3614, -1
	Tsquery = 3615, -1
	GtsVector = 3642, -1
	TsVectorArray = 3643, -1
	GtsVectorArray = 3644, -1
	TsqueryArray = 3645, -1
	Regconfig = 3734, -1
	RegconfigArray = 3735, -1
	Regdictionary = 3769, -1
	RegdictionaryArray = 3770, -1
	Jsonb = 3802, -1
	JsonbArray = 3807, -1
	AnyRange = 3831, -1
	EventTrigger = 3838, -1
	Int4Range = 3904, -1
	Int4RangeArray = 3905, -1
	NumRange = 3906, -1
	NumRangeArray = 3907, -1
	TsRange = 3908, -1
	TsRangeArray = 3909, -1
	TstzRange = 3910, -1
	TstzRangeArray = 3911, -1
	DateRange = 3912, -1
	DateRangeArray = 3913, -1
	Int8Range = 3926, -1
	Int8RangeArray = 3927, -1
	Jsonpath = 4072, -1
	JsonpathArray = 4073, -1
	Regnamespace = 4089, -1
	RegnamespaceArray = 4090, -1
	Regrole = 4096, -1
	RegroleArray = 4097, -1
	Regcollation = 4191, -1
	RegcollationArray = 4192, -1
	Int4multiRange = 4451, -1
	NummultiRange = 4532, -1
	TsmultiRange = 4533, -1
	TstzmultiRange = 4534, -1
	DatemultiRange = 4535, -1
	Int8multiRange = 4536, -1
	AnymultiRange = 4537, -1
	AnycompatiblemultiRange = 4538, -1
	PgBrinBloomSummary = 4600, -1
	PgBrinMinmaxMultiSummary = 4601, -1
	PgMcvList = 5017, -1
	PgSnapshot = 5038, -1
	PgSnapshotArray = 5039, -1
	Xid8 = 5069, -1
	Anycompatible = 5077, -1
	Anycompatiblearray = 5078, -1
	Anycompatiblenonarray = 5079, -1
	AnycompatibleRange = 5080, -1
	Int4multiRangeArray = 6150, -1
	NummultiRangeArray = 6151, -1
	TsmultiRangeArray = 6152, -1
	TstzmultiRangeArray = 6153, -1
	DatemultiRangeArray = 6155, -1
	Int8multiRangeArray = 6157, -1

}

/// Describes how to format a given value or set of values.
#[derive(Debug, Copy, Clone)]
pub enum FormatCode {
	/// Use the stable text representation.
	Text = 0,
	/// Use the less-stable binary representation.
	Binary = 1,
}

impl TryFrom<i16> for FormatCode {
	type Error = ProtocolError;

	fn try_from(value: i16) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(FormatCode::Text),
			1 => Ok(FormatCode::Binary),
			other => Err(ProtocolError::InvalidFormatCode(other)),
		}
	}
}

#[derive(Debug)]
pub struct Startup {
	pub requested_protocol_version: (i16, i16),
	pub parameters: HashMap<String, String>,
}

#[derive(Debug)]
pub enum Describe {
	Portal(String),
	PreparedStatement(String),
}

#[derive(Debug)]
pub struct Parse {
	pub prepared_statement_name: String,
	pub query: String,
	pub parameter_types: Vec<DataTypeOid>,
}

#[derive(Debug)]
pub enum BindFormat {
	All(FormatCode),
	PerColumn(Vec<FormatCode>),
}

#[derive(Debug)]
pub struct Bind {
	pub portal: String,
	pub prepared_statement_name: String,
	pub result_format: BindFormat,
	pub parameters: Vec<Bytes>,
}

#[derive(Debug)]
pub enum Close {
	Portal(String),
	PreparedStatement(String),
}

// Byte1('B')
// Identifies the message as a Bind command.

// Int32
// Length of message contents in bytes, including self.

// String
// The name of the destination portal (an empty string selects the unnamed portal).

// String
// The name of the source prepared statement (an empty string selects the unnamed prepared statement).

// Int16
// The number of parameter format codes that follow (denoted C below). This can be zero to indicate that there are no parameters or that the parameters all use the default format (text); or one, in which case the specified format code is applied to all parameters; or it can equal the actual number of parameters.

// Int16[C]
// The parameter format codes. Each must presently be zero (text) or one (binary).

// Int16
// The number of parameter values that follow (possibly zero). This must match the number of parameters needed by the query.

// Next, the following pair of fields appear for each parameter:

// Int32
// The length of the parameter value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL parameter value. No value bytes follow in the NULL case.

// Byten
// The value of the parameter, in the format indicated by the associated format code. n is the above length.

// After the last parameter, the following fields appear:

// Int16
// The number of result-column format codes that follow (denoted R below). This can be zero to indicate that there are no result columns or that the result columns should all use the default format (text); or one, in which case the specified format code is applied to all result columns (if any); or it can equal the actual number of result columns of the query.

// Int16[R]
// The result-column format codes. Each must presently be zero (text) or one (binary).

#[derive(Debug)]
pub struct Execute {
	pub portal: String,
	pub max_rows: Option<i32>,
}

#[derive(Debug)]
pub enum ClientMessage {
	SSLRequest, // for SSL negotiation
	Startup(Startup),
	Parse(Parse),
	Describe(Describe),
	Bind(Bind),
	Sync,
	Execute(Execute),
	Query(String),
	Terminate,
	Close(Close),
}

pub trait BackendMessage: std::fmt::Debug {
	const TAG: u8;

	fn encode(&self, dst: &mut BytesMut);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlState {
	SuccessfulCompletion,
	FeatureNotSupported,
	InvalidCursorName,
	ConnectionException,
	InvalidSQLStatementName,
	DataException,
	ProtocolViolation,
	SyntaxError,
	InvalidDatetimeFormat,
}

impl SqlState {
	pub fn code(&self) -> &str {
		match self {
			Self::SuccessfulCompletion => "00000",
			Self::FeatureNotSupported => "0A000",
			Self::InvalidCursorName => "34000",
			Self::ConnectionException => "08000",
			Self::InvalidSQLStatementName => "26000",
			Self::DataException => "22000",
			Self::ProtocolViolation => "08P01",
			Self::SyntaxError => "42601",
			Self::InvalidDatetimeFormat => "22007",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Severity {
	Error,
	Fatal,
}

impl Severity {
	pub fn code(&self) -> &str {
		match self {
			Self::Fatal => "FATAL",
			Self::Error => "ERROR",
		}
	}
}

#[derive(thiserror::Error, Debug, Clone)]
pub struct ErrorResponse {
	pub sql_state: SqlState,
	pub severity: Severity,
	pub message: String,
}

impl ErrorResponse {
	pub fn new(sql_state: SqlState, severity: Severity, message: impl Into<String>) -> Self {
		ErrorResponse {
			sql_state,
			severity,
			message: message.into(),
		}
	}

	pub fn error(sql_state: SqlState, message: impl Into<String>) -> Self {
		Self::new(sql_state, Severity::Error, message)
	}

	pub fn fatal(sql_state: SqlState, message: impl Into<String>) -> Self {
		Self::new(sql_state, Severity::Error, message)
	}
}

impl Display for ErrorResponse {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "error")
	}
}

impl BackendMessage for ErrorResponse {
	const TAG: u8 = b'E';

	fn encode(&self, dst: &mut BytesMut) {
		dst.put_u8(b'C');
		dst.put_slice(self.sql_state.code().as_bytes());
		dst.put_u8(0);
		dst.put_u8(b'S');
		dst.put_slice(self.severity.code().as_bytes());
		dst.put_u8(0);
		dst.put_u8(b'M');
		dst.put_slice(self.message.as_bytes());
		dst.put_u8(0);

		dst.put_u8(0); // tag
	}
}

#[derive(Debug, Clone)]
pub struct StatementDescription {
	pub fields: Option<Vec<FieldDescription>>,
	pub parameters: Option<Vec<DataTypeOid>>,
}

// From https://www.postgresql.org/docs/current/protocol-message-formats.html

// ParameterDescription (B)

//     Byte1('t')
//         Identifies the message as a parameter description.
//
//     Int32
//         Length of message contents in bytes, including self.
//     Int16
//         The number of parameters used by the statement (can be zero).
//
//     Then, for each parameter, there is the following:
//     Int32
//         Specifies the object ID of the parameter data type.

#[derive(Debug, Clone)]
pub struct ParameterDescription {
	pub parameters: Vec<DataTypeOid>,
}

impl BackendMessage for ParameterDescription {
	const TAG: u8 = b't';

	fn encode(&self, dst: &mut BytesMut) {
		dst.put_i16(self.parameters.len() as i16);
		for parameter in &self.parameters {
			dst.put_u32((*parameter).into());
		}
	}
}

#[derive(Debug, Clone)]
pub struct FieldDescription {
	pub name: String,
	pub data_type: DataTypeOid,
}

// From https://www.postgresql.org/docs/current/protocol-message-formats.html
//
// RowDescription (B)

//     Byte1('T')

//         Identifies the message as a row description.
//     Int32

//         Length of message contents in bytes, including self.
//     Int16

//         Specifies the number of fields in a row (can be zero).

//     Then, for each field, there is the following:

//     String

//         The field name.
//     Int32

//         If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
//     Int16

//         If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
//     Int32

//         The object ID of the field's data type.
//     Int16

//         The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
//     Int32

//         The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
//     Int16

//         The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.

#[derive(Debug, Clone)]
pub struct RowDescription {
	pub fields: Vec<FieldDescription>,
	pub format_code: FormatCode,
}

impl BackendMessage for RowDescription {
	const TAG: u8 = b'T';

	fn encode(&self, dst: &mut BytesMut) {
		dst.put_i16(self.fields.len() as i16);
		for field in &self.fields {
			dst.put_slice(field.name.as_bytes());
			dst.put_u8(0);
			dst.put_i32(0); // table oid
			dst.put_i16(0); // column attr number
			dst.put_u32(field.data_type.into());
			dst.put_i16(field.data_type.size_bytes());
			dst.put_i32(-1); // data type modifier
			dst.put_i16(self.format_code as i16);
		}
	}
}

#[derive(Debug)]
pub struct AuthenticationOk;

impl BackendMessage for AuthenticationOk {
	const TAG: u8 = b'R';

	fn encode(&self, dst: &mut BytesMut) {
		dst.put_i32(0);
	}
}

#[derive(Debug)]
pub struct ReadyForQuery;

impl BackendMessage for ReadyForQuery {
	const TAG: u8 = b'Z';

	fn encode(&self, dst: &mut BytesMut) {
		dst.put_u8(b'I');
	}
}

#[derive(Debug)]
pub struct ParseComplete;

impl BackendMessage for ParseComplete {
	const TAG: u8 = b'1';

	fn encode(&self, _dst: &mut BytesMut) {}
}

#[derive(Debug)]
pub struct BindComplete;

impl BackendMessage for BindComplete {
	const TAG: u8 = b'2';

	fn encode(&self, _dst: &mut BytesMut) {}
}

#[derive(Debug)]
pub struct NoData;

impl BackendMessage for NoData {
	const TAG: u8 = b'n';

	fn encode(&self, _dst: &mut BytesMut) {}
}

#[derive(Debug)]
pub struct EmptyQueryResponse;

impl BackendMessage for EmptyQueryResponse {
	const TAG: u8 = b'I';

	fn encode(&self, _dst: &mut BytesMut) {}
}

#[derive(Debug)]
pub struct CommandComplete {
	pub command_tag: String,
}

impl BackendMessage for CommandComplete {
	const TAG: u8 = b'C';

	fn encode(&self, dst: &mut BytesMut) {
		dst.put_slice(self.command_tag.as_bytes());
		dst.put_u8(0);
	}
}

#[derive(Debug)]
pub struct ParameterStatus {
	name: String,
	value: String,
}

impl BackendMessage for ParameterStatus {
	const TAG: u8 = b'S';

	fn encode(&self, dst: &mut BytesMut) {
		dst.put_slice(self.name.as_bytes());
		dst.put_u8(0);
		dst.put_slice(self.value.as_bytes());
		dst.put_u8(0);
	}
}

impl ParameterStatus {
	pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
		Self {
			name: name.into(),
			value: value.into(),
		}
	}
}

#[derive(Default, Debug)]
pub struct ConnectionCodec {
	// most state tracking is handled at a higher level
	// however, the actual wire format uses a different header for startup vs normal messages
	// so we need to be able to differentiate inside the decoder
	startup_received: bool,
}

impl ConnectionCodec {
	pub fn new() -> Self {
		Self {
			startup_received: false,
		}
	}
}

#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
	#[error("io error: {0}")]
	Io(#[from] std::io::Error),
	#[error("utf8 error: {0}")]
	Utf8(#[from] std::string::FromUtf8Error),
	#[error("parsing error")]
	ParserError,
	#[error("invalid message type: {0}")]
	InvalidMessageType(u8),
	#[error("invalid format code: {0}")]
	InvalidFormatCode(i16),
}

// length prefix, two version components
const STARTUP_HEADER_SIZE: usize = size_of::<i32>() + (size_of::<i16>() * 2);
// message tag, length prefix
const MESSAGE_HEADER_SIZE: usize = size_of::<u8>() + size_of::<i32>();

impl Decoder for ConnectionCodec {
	type Item = ClientMessage;
	type Error = ProtocolError;

	fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
		if !self.startup_received {
			if src.len() < STARTUP_HEADER_SIZE {
				return Ok(None);
			}

			let mut header_buf = src.clone();
			let message_len = header_buf.get_i32() as usize;
			let protocol_version_major = header_buf.get_i16();
			let protocol_version_minor = header_buf.get_i16();

			if protocol_version_major == 1234i16 && protocol_version_minor == 5679i16 {
				src.advance(STARTUP_HEADER_SIZE);
				return Ok(Some(ClientMessage::SSLRequest));
			}

			if src.len() < message_len {
				src.reserve(message_len - src.len());
				return Ok(None);
			}

			src.advance(STARTUP_HEADER_SIZE);

			let mut parameters = HashMap::new();

			let mut param_str_start_pos = 0;
			let mut current_key = None;
			for (i, &blah) in src.iter().enumerate() {
				if blah == 0 {
					let string_value = String::from_utf8(src[param_str_start_pos..i].to_owned())?;
					param_str_start_pos = i + 1;

					current_key = match current_key {
						Some(key) => {
							parameters.insert(key, string_value);
							None
						}
						None => Some(string_value),
					}
				}
			}

			src.advance(message_len - STARTUP_HEADER_SIZE);

			self.startup_received = true;
			return Ok(Some(ClientMessage::Startup(Startup {
				requested_protocol_version: (protocol_version_major, protocol_version_minor),
				parameters,
			})));
		}

		if src.len() < MESSAGE_HEADER_SIZE {
			src.reserve(MESSAGE_HEADER_SIZE);
			return Ok(None);
		}

		let mut header_buf = src.clone();
		let message_tag = header_buf.get_u8();
		let message_len = header_buf.get_i32() as usize;

		if src.len() < message_len {
			src.reserve(message_len - src.len());
			return Ok(None);
		}

		src.advance(MESSAGE_HEADER_SIZE);

		let read_cstr = |src: &mut BytesMut| -> Result<String, ProtocolError> {
			let next_null = src.iter().position(|&b| b == 0).ok_or(ProtocolError::ParserError)?;
			let bytes = src[..next_null].to_owned();
			src.advance(bytes.len() + 1);
			Ok(String::from_utf8(bytes)?)
		};

		let message = match message_tag {
			b'P' => {
				// 	Byte1('P')
				// 	  Identifies the message as a Parse command.
				//
				// Int32
				//   Length of message contents in bytes, including self.
				// String
				// 	The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
				// String
				// 	The query string to be parsed.
				// Int16
				//   The number of parameter data types specified (can be zero).
				// 	 Note that this is not an indication of the number of parameters that might appear in the query string, only the number that the frontend wants to prespecify types for.
				// 	 Then, for each parameter, there is the following:
				// Int32
				// 	Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
				tracing::debug!("PARSE.src {:?}", &src);
				let prepared_statement_name = read_cstr(src)?;
				let query = read_cstr(src)?;
				// pgbench seems to send zero as a single byte if the zero is the last element
				let num_params = if src.len() == 1 {
					src.get_i8() as i16
				} else {
					src.get_i16()
				};

				let params: Vec<DataTypeOid> = (0..num_params).map(|_| DataTypeOid::from(src.get_u32())).collect();

				ClientMessage::Parse(Parse {
					prepared_statement_name,
					query,
					parameter_types: Vec::new(),
				})
			}
			b'D' => {
				// 	Byte1('D')
				// 	Identifies the message as a Describe command.

				// Int32
				// 	Length of message contents in bytes, including self.

				// Byte1
				// 	'S' to describe a prepared statement; or 'P' to describe a portal.

				// String
				// 	The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared statement or portal).
				tracing::debug!("ClientMessage::Describe src {:?}", &src);

				let target_type = src.get_u8();
				let name = read_cstr(src).unwrap_or("".to_string());

				ClientMessage::Describe(match target_type {
					b'P' => Describe::Portal(name),
					b'S' => Describe::PreparedStatement(name),
					_ => return Err(ProtocolError::ParserError),
				})
			}
			b'S' => ClientMessage::Sync,
			b'B' => {
				tracing::debug!("BIND.src {:?}", &src);

				let portal = read_cstr(src)?;
				let prepared_statement_name = read_cstr(src)?;

				let num_param_format_codes = src.get_i16();
				for _ in 0..num_param_format_codes {
					let _format_code = src.get_i16();
				}

				let mut parameters: Vec<Bytes> = vec![];
				let num_params = src.get_i16();
				for _ in 0..num_params {
					let param_len = src.get_i32() as usize;
					let _bytes = &src[0..param_len];

					let b = Bytes::copy_from_slice(_bytes);
					parameters.push(b);

					src.advance(param_len);
				}

				let result_format = match src.get_i16() {
					0 => BindFormat::All(FormatCode::Text),
					1 => {
						// pgbench does not send enough bytes
						let code = if src.len() == 1 {
							src.get_i8() as i16
						} else {
							src.get_i16()
						};

						let format_code = FormatCode::try_from(code)?;
						BindFormat::All(format_code.into())
					}
					n => {
						let mut result_format_codes = Vec::new();
						for _ in 0..n {
							result_format_codes.push(src.get_i16().try_into()?);
						}
						BindFormat::PerColumn(result_format_codes)
					}
				};

				ClientMessage::Bind(Bind {
					portal,
					prepared_statement_name,
					result_format,
					parameters,
				})
			}
			b'E' => {
				// Byte1('E')
				//   Identifies the message as an Execute command.
				// Int32
				//   Length of message contents in bytes, including self.
				// String
				// 	 The name of the portal to execute (an empty string selects the unnamed portal).
				// Int32
				//   Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.

				tracing::debug!("EXECUTE.src {:?}", &src);

				let portal = read_cstr(src)?;

				let max_rows = if src.is_empty() { None } else { Some(src.get_i32()) };
				ClientMessage::Execute(Execute { portal, max_rows })
			}
			b'Q' => {
				let query = read_cstr(src)?;
				ClientMessage::Query(query)
			}
			b'X' => ClientMessage::Terminate,
			b'C' => {
				let target_type = src.get_u8();
				let name = read_cstr(src)?;

				ClientMessage::Close(match target_type {
					b'P' => Close::Portal(name),
					b'S' => Close::PreparedStatement(name),
					_ => return Err(ProtocolError::ParserError),
				})
			}
			other => {
				println!("unknown message type: {:?}", other);
				return Err(ProtocolError::InvalidMessageType(other));
			}
		};

		Ok(Some(message))
	}
}

impl<T: BackendMessage> Encoder<T> for ConnectionCodec {
	type Error = ProtocolError;

	fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
		let mut body = BytesMut::new();
		item.encode(&mut body);

		dst.put_u8(T::TAG);
		dst.put_i32((body.len() + 4) as i32);
		dst.put_slice(&body);
		Ok(())
	}
}

pub struct SSLResponse(pub bool);

impl Encoder<SSLResponse> for ConnectionCodec {
	type Error = ProtocolError;

	fn encode(&mut self, item: SSLResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
		dst.put_u8(if item.0 { b'S' } else { b'N' });
		Ok(())
	}
}
