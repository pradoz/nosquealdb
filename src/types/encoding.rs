use super::AttributeValue;
use std::collections::{BTreeMap, BTreeSet};

#[repr(u8)]
enum TypeTag {
    S = 1,
    N = 2,
    B = 3,
    Bool = 4,
    Null = 5,
    M = 6,
    L = 7,
    Ss = 8,
    Ns = 9,
    Bs = 10,
}

impl TryFrom<u8> for TypeTag {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::S),
            2 => Ok(Self::N),
            3 => Ok(Self::B),
            4 => Ok(Self::Bool),
            5 => Ok(Self::Null),
            6 => Ok(Self::M),
            7 => Ok(Self::L),
            8 => Ok(Self::Ss),
            9 => Ok(Self::Ns),
            10 => Ok(Self::Bs),
            _ => Err(DecodeError::InvalidTypeTag(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    UnexpectedEof,
    InvalidUtf8,
    InvalidTypeTag(u8),
    InvalidBool(u8),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected end of data"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 string"),
            Self::InvalidTypeTag(t) => write!(f, "invalid type tag: {t}"),
            Self::InvalidBool(b) => write!(f, "invalid bool value: {b}"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    pub fn encode(mut self, value: &AttributeValue) -> Vec<u8> {
        self.write_value(value);
        self.buf
    }

    fn write_len(&mut self, len: usize) {
        // variable length encoding
        // len < 128: 1 byte
        // len >= 128: 4 bytes
        if len < 128 {
            self.buf.push(len as u8);
        } else {
            // byte 0: 0x80 flag + top 7 bits of len
            self.buf.push(0x80 | ((len >> 24) as u8 & 0x7F));
            // bytes 1-3: remaining 24 bits
            self.buf.push((len >> 16) as u8);
            self.buf.push((len >> 8) as u8);
            self.buf.push(len as u8);
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        self.write_len(bytes.len());
        self.buf.extend_from_slice(bytes);
    }

    fn write_value(&mut self, value: &AttributeValue) {
        match value {
            AttributeValue::S(s) => {
                self.buf.push(TypeTag::S as u8);
                self.write_bytes(s.as_bytes());
            }
            AttributeValue::N(n) => {
                self.buf.push(TypeTag::N as u8);
                self.write_bytes(n.as_bytes());
            }
            AttributeValue::B(b) => {
                self.buf.push(TypeTag::B as u8);
                self.write_bytes(b);
            }
            AttributeValue::Bool(b) => {
                self.buf.push(TypeTag::Bool as u8);
                self.buf.push(if *b { 1 } else { 0 });
            }
            AttributeValue::Null => self.buf.push(TypeTag::Null as u8),
            AttributeValue::M(m) => {
                self.buf.push(TypeTag::M as u8);
                self.write_len(m.len());
                for (k, v) in m {
                    self.write_bytes(k.as_bytes());
                    self.write_value(v);
                }
            }
            AttributeValue::L(l) => {
                self.buf.push(TypeTag::L as u8);
                self.write_len(l.len());
                for v in l {
                    self.write_value(v);
                }
            }
            AttributeValue::Ss(ss) => {
                self.buf.push(TypeTag::Ss as u8);
                self.write_len(ss.len());
                for s in ss {
                    self.write_bytes(s.as_bytes());
                }
            }
            AttributeValue::Ns(ns) => {
                self.buf.push(TypeTag::Ns as u8);
                self.write_len(ns.len());
                for n in ns {
                    self.write_bytes(n.as_bytes());
                }
            }
            AttributeValue::Bs(bs) => {
                self.buf.push(TypeTag::Bs as u8);
                self.write_len(bs.len());
                for b in bs {
                    self.write_bytes(b);
                }
            }
        }
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn decode(mut self) -> Result<AttributeValue, DecodeError> {
        self.read_value()
    }

    fn read_u8(&mut self) -> Result<u8, DecodeError> {
        if self.pos >= self.data.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_len(&mut self) -> Result<usize, DecodeError> {
        let first = self.read_u8()?;
        if first & 0x80 == 0 {
            Ok(first as usize)
        } else {
            let b1 = (first & 0x7F) as usize;
            let b2 = self.read_u8()? as usize;
            let b3 = self.read_u8()? as usize;
            let b4 = self.read_u8()? as usize;
            Ok((b1 << 24) | (b2 << 16) | (b3 << 8) | b4)
        }
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.read_len()?;
        if self.pos + len > self.data.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        let bytes = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(bytes)
    }

    fn read_value(&mut self) -> Result<AttributeValue, DecodeError> {
        let tag = TypeTag::try_from(self.read_u8()?)?;

        match tag {
            TypeTag::S => {
                let bytes = self.read_bytes()?;
                let s = String::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)?;
                Ok(AttributeValue::S(s))
            }
            TypeTag::N => {
                let bytes = self.read_bytes()?;
                let n = String::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)?;
                Ok(AttributeValue::N(n))
            }
            TypeTag::B => {
                let bytes = self.read_bytes()?;
                Ok(AttributeValue::B(bytes))
            }
            TypeTag::Bool => {
                let b = self.read_u8()?;
                match b {
                    0 => Ok(AttributeValue::Bool(false)),
                    1 => Ok(AttributeValue::Bool(true)),
                    _ => Err(DecodeError::InvalidBool(b)),
                }
            }
            TypeTag::Null => Ok(AttributeValue::Null),
            TypeTag::M => {
                let len = self.read_len()?;
                let mut m = BTreeMap::new();
                for _ in 0..len {
                    let key_bytes = self.read_bytes()?;
                    let key = String::from_utf8(key_bytes).map_err(|_| DecodeError::InvalidUtf8)?;
                    let value = self.read_value()?;
                    m.insert(key, value);
                }
                Ok(AttributeValue::M(m))
            }
            TypeTag::L => {
                let len = self.read_len()?;
                let mut l = Vec::with_capacity(len);
                for _ in 0..len {
                    l.push(self.read_value()?);
                }
                Ok(AttributeValue::L(l))
            }
            TypeTag::Ss => {
                let len = self.read_len()?;
                let mut ss = BTreeSet::new();
                for _ in 0..len {
                    let bytes = self.read_bytes()?;
                    let s = String::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)?;
                    ss.insert(s);
                }
                Ok(AttributeValue::Ss(ss))
            }
            TypeTag::Ns => {
                let len = self.read_len()?;
                let mut ns = BTreeSet::new();
                for _ in 0..len {
                    let bytes = self.read_bytes()?;
                    let n = String::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)?;
                    ns.insert(n);
                }
                Ok(AttributeValue::Ns(ns))
            }
            TypeTag::Bs => {
                let len = self.read_len()?;
                let mut bs = BTreeSet::new();
                for _ in 0..len {
                    bs.insert(self.read_bytes()?);
                }
                Ok(AttributeValue::Bs(bs))
            }
        }
    }
}

pub fn encode(value: &AttributeValue) -> Vec<u8> {
    Encoder::new().encode(value)
}

pub fn decode(data: &[u8]) -> Result<AttributeValue, DecodeError> {
    Decoder::new(data).decode()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_large_length() {
        // large: >127 bytes
        let data = "x".repeat(200);
        let enc = encode(&AttributeValue::S(data.clone()));
        let dec = decode(&enc).unwrap();
        assert_eq!(AttributeValue::S(data), dec);
    }

    #[test]
    fn decode_truncated_fails() {
        let enc = encode(&AttributeValue::S("hello".into()));
        let truncated = &enc[..enc.len() - 2];
        assert!(decode(truncated).is_err());
    }

    #[test]
    fn decode_invalid_tag_fails() {
        assert!(decode(&[255]).is_err());
    }

    mod roundtrip {
        use super::*;

        fn roundtrip(value: AttributeValue) {
            let enc = encode(&value);
            let dec = decode(&enc).expect("decode failed");
            assert_eq!(value, dec, "roundtrip failed for {:?}", value);
        }

        #[test]
        fn scalars() {
            roundtrip(AttributeValue::S("".into()));
            roundtrip(AttributeValue::S("hello".into()));
            roundtrip(AttributeValue::N("1234".into()));
            roundtrip(AttributeValue::N("-99.99".into()));
            roundtrip(AttributeValue::B(vec![]));
            roundtrip(AttributeValue::B(vec![1, 2, 3, 4]));
            roundtrip(AttributeValue::Bool(true));
            roundtrip(AttributeValue::Bool(false));
            roundtrip(AttributeValue::Null);
        }

        #[test]
        fn documents() {
            roundtrip(AttributeValue::S("".into()));
        }

        #[test]
        fn sets() {
            roundtrip(AttributeValue::S("".into()));
        }

        #[test]
        fn nested() {
            roundtrip(AttributeValue::S("".into()));
        }
    }
}
