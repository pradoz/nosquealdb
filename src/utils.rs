use crate::TableError;
use crate::error::EvalError;
use crate::types::{AttributeValue, KeyValue};
use std::borrow::Cow;
use std::cmp::Ordering;

pub fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0F) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3F] as char);
        } else {
            result.push('=');
        }
    }
    result
}

pub fn base64_decode(input: &str) -> Option<Vec<u8>> {
    const DECODE: [i8; 128] = [
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1,
        -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4,
        5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1,
        -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
        46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
    ];

    let input = input.trim_end_matches('=');
    let mut result = Vec::with_capacity(input.len() * 3 / 4);

    let chars: Vec<u8> = input
        .chars()
        .filter_map(|c| {
            let c = c as usize;
            if c < 128 {
                let val = DECODE[c];
                if val >= 0 {
                    return Some(val as u8);
                }
            }
            None
        })
        .collect();

    if chars.len() != input.len() {
        return None;
    }

    for chunk in chars.chunks(4) {
        match chunk.len() {
            4 => {
                result.push((chunk[0] << 2) | (chunk[1] >> 4));
                result.push((chunk[1] << 4) | (chunk[2] >> 2));
                result.push((chunk[2] << 6) | chunk[3]);
            }
            3 => {
                result.push((chunk[0] << 2) | (chunk[1] >> 4));
                result.push((chunk[1] << 4) | (chunk[2] >> 2));
            }
            2 => {
                result.push((chunk[0] << 2) | (chunk[1] >> 4));
            }
            _ => return None,
        }
    }

    Some(result)
}

#[inline]
pub fn compare_key_values(a: &KeyValue, b: &KeyValue) -> Ordering {
    match (a, b) {
        (KeyValue::S(a), KeyValue::S(b)) => a.cmp(b),
        (KeyValue::N(a), KeyValue::N(b)) => compare_numeric_strings(a, b),
        (KeyValue::B(a), KeyValue::B(b)) => a.cmp(b),
        // different types: compare by type name for consistent ordering
        _ => a.type_name().cmp(b.type_name()),
    }
}

#[inline]
pub fn compare_values(a: &AttributeValue, b: &AttributeValue) -> Result<Ordering, EvalError> {
    match (a, b) {
        (AttributeValue::S(a), AttributeValue::S(b)) => Ok(a.cmp(b)),
        (AttributeValue::N(a), AttributeValue::N(b)) => Ok(compare_numeric_strings(a, b)),
        (AttributeValue::B(a), AttributeValue::B(b)) => Ok(a.cmp(b)),
        _ => Err(EvalError::TypeMismatch {
            left: a.type_name(),
            right: b.type_name(),
        }),
    }
}

#[inline]
pub fn compare_numeric_strings(a: &str, b: &str) -> Ordering {
    // try integer comparison first for exact precision
    if let (Ok(x), Ok(y)) = (a.parse::<i64>(), b.parse::<i64>()) {
        return x.cmp(&y);
    }

    // fall back to float comparison
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
        // if parsing fails, fall back to string comparison
        _ => a.cmp(b),
    }
}

#[inline]
pub fn numbers_equal(a: &str, b: &str) -> bool {
    compare_numeric_strings(a, b) == Ordering::Equal
}

pub fn add_numeric_strings(a: &str, b: &str) -> Result<String, TableError> {
    // try integer arithmetic first for exact precision
    if let (Ok(x), Ok(y)) = (a.parse::<i64>(), b.parse::<i64>()) {
        return Ok((x + y).to_string());
    }

    // fall back to float arithmetic
    let x: f64 = a
        .parse()
        .map_err(|_| TableError::update_error("invalid number"))?;
    let y: f64 = b
        .parse()
        .map_err(|_| TableError::update_error("invalid number"))?;
    Ok((x + y).to_string())
}

const ESCAPE_CHARS: [char; 3] = ['#', ':', '\\'];

/// Escape special characters in storage keys.
/// Returns Cow::Borrowed if no escaping needed (common case optimization).
#[inline]
pub fn escape_key_chars(s: &str) -> Cow<'_, str> {
    // Fast path: check if any escaping is needed
    if !s.contains(|c| ESCAPE_CHARS.contains(&c)) {
        return Cow::Borrowed(s);
    }

    // Slow path: build escaped string
    let mut result = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '#' => result.push_str("\\#"),
            ':' => result.push_str("\\:"),
            '\\' => result.push_str("\\\\"),
            _ => result.push(c),
        }
    }
    Cow::Owned(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    mod base64 {
        use super::*;

        #[test]
        fn base64_roundtrip() {
            let cases: &[&[u8]] = &[
                b"",
                b"f",
                b"fo",
                b"foo",
                b"foob",
                b"fooba",
                b"foobar",
                &[0, 1, 2, 3, 255, 254, 253],
            ];

            for case in cases {
                let encoded = base64_encode(case);
                let decoded = base64_decode(&encoded).unwrap();
                assert_eq!(*case, decoded.as_slice(), "roundtrip failed for {:?}", case);
            }
        }

        #[test]
        fn base64_known_values() {
            assert_eq!(base64_encode(b""), "");
            assert_eq!(base64_encode(b"f"), "Zg==");
            assert_eq!(base64_encode(b"fo"), "Zm8=");
            assert_eq!(base64_encode(b"foo"), "Zm9v");
            assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
            assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
            assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        }

        #[test]
        fn base64_decode_invalid() {
            assert!(base64_decode("!!!").is_none());
            assert!(base64_decode("abc!").is_none());
        }
    }

    mod add_numeric {
        use super::*;

        #[test]
        fn integers() {
            assert_eq!(add_numeric_strings("10", "5").unwrap(), "15");
            assert_eq!(add_numeric_strings("10", "-5").unwrap(), "5");
            assert_eq!(add_numeric_strings("-10", "-5").unwrap(), "-15");
        }

        #[test]
        fn float() {
            assert_eq!(add_numeric_strings("10", "0.5").unwrap(), "10.5");
            assert_eq!(add_numeric_strings("10.5", "-5.5").unwrap(), "5");
            assert_eq!(add_numeric_strings("-10.2", "-5.3").unwrap(), "-15.5");
        }

        #[test]
        fn invalid_fails() {
            assert!(add_numeric_strings("apple", "0.5").is_err());
            assert!(add_numeric_strings("10.5", "banana").is_err());
        }
    }
}
