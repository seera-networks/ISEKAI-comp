// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub mod config;
pub mod grpc;
pub mod img;
pub mod module;
pub mod policy;
pub mod shared;
#[cfg(not(target_os = "wasi"))]
pub mod yak;

use arrow::datatypes::{Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_array::array::ArrayRef;
use arrow_flight::FlightData;
use futures::ready;
use futures::stream::Stream;
use prost::bytes::{Bytes, BytesMut};
use prost::Message;

use serde::{Deserialize, Serialize};

pub struct FlightDataStream {
    body_stream: Pin<Box<dyn Stream<Item = arrow_flight::error::Result<Bytes>> + Send + 'static>>,
    buffer: Option<BytesMut>,
}

impl FlightDataStream {
    pub fn new(
        body_stream: impl Stream<Item = arrow_flight::error::Result<Bytes>> + Send + 'static,
    ) -> Self {
        Self {
            body_stream: Box::pin(body_stream),
            buffer: None,
        }
    }
}

impl Stream for FlightDataStream {
    type Item = arrow_flight::error::Result<FlightData>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        'outer: loop {
            return match ready!(this.body_stream.as_mut().poll_next(cx)) {
                Some(Ok(bytes)) => {
                    let mut buf = if let Some(mut buf) = this.buffer.take() {
                        buf.extend_from_slice(&bytes);
                        buf
                    } else {
                        let mut buf = BytesMut::with_capacity(bytes.len());
                        buf.extend_from_slice(&bytes);
                        buf
                    };
                    let mut len = prost::decode_length_delimiter(&buf[0..]).unwrap();
                    len += prost::length_delimiter_len(len);
                    if buf.len() < len {
                        this.buffer = Some(buf);
                        continue 'outer;
                    }
                    let flight_data = FlightData::decode_length_delimited(&buf[0..]).unwrap();
                    let _ = buf.split_to(len);
                    this.buffer = Some(buf);
                    Poll::Ready(Some(Ok(flight_data)))
                }
                Some(Err(err)) => Poll::Ready(Some(Err(err))),
                None => Poll::Ready(None),
            };
        }
    }
}

pub fn from_arrow_to_vec(fields: &Vec<Field>, columns: &Vec<ArrayRef>) -> Result<Vec<u8>, String> {
    let batch = match RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), columns.clone()) {
        Err(e) => return Err(format!("failed to new RecordBatch: {:?}", e)),
        Ok(v) => v,
    };
    let buffer = Vec::<u8>::new();
    let mut writer = match StreamWriter::try_new(buffer, &batch.schema()) {
        Err(e) => return Err(format!("failed to new StreamWriter: {:?}", e)),
        Ok(v) => v,
    };
    match writer.write(&batch) {
        Err(e) => return Err(format!("failed to write writer: {:?}", e)),
        Ok(v) => v,
    };
    let res = match writer.into_inner() {
        Err(e) => return Err(format!("failed to get innner: {:?}", e)),
        Ok(v) => v,
    };

    return Ok(res);
}

pub fn from_vec_to_arrow(data: &Vec<u8>) -> Result<Vec<(Field, ArrayRef)>, String> {
    let reader = match StreamReader::try_new(Cursor::new(data), None) {
        Err(e) => return Err(format!("failed to create StreamReader: {:?}", e)),
        Ok(v) => v,
    };

    let mut res = Vec::<(Field, ArrayRef)>::new();

    for record_r in reader {
        let record = match record_r {
            Err(e) => return Err(format!("failed to read RecordBatch: {:?}", e)),
            Ok(v) => v,
        };
        for field in record.schema().fields().iter() {
            let column = match record.column_by_name(field.name()) {
                None => continue,
                Some(v) => v,
            };

            res.push((field.as_ref().clone(), column.clone()))
        }
    }

    Ok(res)
}

#[derive(Debug, Clone)]
pub struct FunctionPolicyDef {
    num: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionPolicyError;
impl std::fmt::Display for FunctionPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "condition is not satisfied")
    }
}

impl FunctionPolicyDef {
    pub fn new(num: &str) -> Self {
        return FunctionPolicyDef {
            num: num.to_string(),
        };
    }

    fn get_num(&self) -> (NumOrd, i64) {
        let mut cs = self.num.chars();
        let value_str = self.num.replace('<', "").replace('>', "").replace('=', "");
        let value = match value_str.parse() {
            Err(_) => return (NumOrd::UN, 0),
            Ok(v) => v,
        };

        let first = match cs.nth(0) {
            None => return (NumOrd::UN, 0),
            Some(c) => c,
        };
        if first == '=' {
            let sec = match cs.nth(0) {
                None => return (NumOrd::UN, 0),
                Some(c) => c,
            };
            if sec == '<' {
                return (NumOrd::LE, value);
            } else if sec == '>' {
                return (NumOrd::GE, value);
            } else {
                return (NumOrd::EQ, value);
            }
        } else if first == '<' {
            return (NumOrd::LT, value);
        } else if first == '>' {
            return (NumOrd::GT, value);
        } else {
            return (NumOrd::UN, 0);
        }
    }

    pub fn assert_value(&self, value: i64) -> Result<bool, FunctionPolicyError> {
        let (ord, v) = self.get_num();
        match ord {
            NumOrd::GT => {
                if value <= v {
                    return Ok(false);
                }
            }
            NumOrd::GE => {
                if value < v {
                    return Ok(false);
                }
            }
            NumOrd::EQ => {
                if value != v {
                    return Ok(false);
                }
            }
            NumOrd::LE => {
                if value > v {
                    return Ok(false);
                }
            }
            NumOrd::LT => {
                if value >= v {
                    return Ok(false);
                }
            }
            NumOrd::UN => return Err(FunctionPolicyError {}),
        }

        Ok(true)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NumOrd {
    GT, // greater than
    GE, // greater than or equal
    EQ, // equal
    LE, // less than or equal
    LT, // less than
    UN, // unknwon
}

#[cfg(test)]
mod tests {
    use super::{FunctionPolicyDef, NumOrd};

    #[test]
    fn get_num() {
        let d = FunctionPolicyDef {
            num: ">100".to_string(),
        };
        let (ord, value) = d.get_num();
        assert_eq!(ord, NumOrd::GT);
        assert_eq!(value, 100);
        assert_eq!(d.assert_value(101), Ok(true));
        assert_eq!(d.assert_value(100), Ok(false));

        let d = FunctionPolicyDef {
            num: "<100".to_string(),
        };
        let (ord, value) = d.get_num();
        assert_eq!(ord, NumOrd::LT);
        assert_eq!(value, 100);
        assert_eq!(d.assert_value(99), Ok(true));
        assert_eq!(d.assert_value(100), Ok(false));

        let d = FunctionPolicyDef {
            num: "=100".to_string(),
        };
        let (ord, value) = d.get_num();
        assert_eq!(ord, NumOrd::EQ);
        assert_eq!(value, 100);
        assert_eq!(d.assert_value(100), Ok(true));
        assert_eq!(d.assert_value(101), Ok(false));

        let d = FunctionPolicyDef {
            num: "=<100".to_string(),
        };
        let (ord, value) = d.get_num();
        assert_eq!(ord, NumOrd::LE);
        assert_eq!(value, 100);
        assert_eq!(d.assert_value(99), Ok(true));
        assert_eq!(d.assert_value(100), Ok(true));
        assert_eq!(d.assert_value(101), Ok(false));

        let d = FunctionPolicyDef {
            num: "=>100".to_string(),
        };
        let (ord, value) = d.get_num();
        assert_eq!(ord, NumOrd::GE);
        assert_eq!(value, 100);
        assert_eq!(d.assert_value(101), Ok(true));
        assert_eq!(d.assert_value(100), Ok(true));
        assert_eq!(d.assert_value(99), Ok(false));
    }
}
