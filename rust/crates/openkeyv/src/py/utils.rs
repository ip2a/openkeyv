use crate::value::{Value, ValueKind};
use pyo3::BoundObject;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyList};

const STRUCTURED_MAGIC: &[u8; 4] = b"OKV1";
const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_INT: u8 = 3;
const TAG_FLOAT: u8 = 4;
const TAG_STR: u8 = 5;
const TAG_BYTES: u8 = 6;
const TAG_LIST: u8 = 7;
const TAG_DICT: u8 = 8;

pub fn value_to_py(py: Python, value: &Value) -> PyResult<Py<PyAny>> {
    match value.kind() {
        ValueKind::Binary => Ok(PyBytes::new(py, value.bytes()).into_any().unbind()),
        ValueKind::Utf8 => {
            let text = std::str::from_utf8(value.bytes()).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid UTF-8 value: {e}"))
            })?;
            Ok(text.into_pyobject(py)?.into_any().unbind())
        }
        ValueKind::Integer => {
            let bytes = fixed_bytes::<8>(value.bytes(), "integer")?;
            Ok(i64::from_le_bytes(bytes)
                .into_pyobject(py)?
                .into_any()
                .unbind())
        }
        ValueKind::Float => {
            let bytes = fixed_bytes::<8>(value.bytes(), "float")?;
            Ok(f64::from_le_bytes(bytes)
                .into_pyobject(py)?
                .into_any()
                .unbind())
        }
        ValueKind::Bool => match value.bytes().as_ref() {
            [0] => Ok(PyBool::new(py, false).into_any().unbind()),
            [1] => Ok(PyBool::new(py, true).into_any().unbind()),
            _ => Err(pyo3::exceptions::PyValueError::new_err(
                "invalid bool payload",
            )),
        },
        ValueKind::Null => Ok(py.None()),
        ValueKind::Structured => decode_structured(py, value.bytes()),
    }
}

pub fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        Ok(Value::null())
    } else if let Ok(bytes) = obj.cast::<PyBytes>() {
        Ok(Value::binary(bytes.as_bytes().to_vec()))
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(Value::bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(Value::integer(i))
    } else if let Ok(float) = obj.cast::<PyFloat>() {
        Ok(Value::float(float.value()))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(Value::utf8(s))
    } else if obj.cast::<PyList>().is_ok() || obj.cast::<PyDict>().is_ok() {
        let mut out = Vec::with_capacity(STRUCTURED_MAGIC.len() + 32);
        out.extend_from_slice(STRUCTURED_MAGIC);
        encode_structured_value(obj, &mut out)?;
        Ok(Value::structured(out))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "unsupported value type; expected dict, list, str, bytes, int, float, bool, or None",
        ))
    }
}

pub fn optional_value_to_py(py: Python, value: Option<Value>) -> PyResult<Py<PyAny>> {
    match value {
        Some(value) => value_to_py(py, &value),
        None => Ok(py.None()),
    }
}

fn fixed_bytes<const N: usize>(bytes: &[u8], label: &str) -> PyResult<[u8; N]> {
    bytes.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid {label} payload length"))
    })
}

fn encode_structured_value(obj: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    if obj.is_none() {
        out.push(TAG_NULL);
    } else if let Ok(bytes) = obj.cast::<PyBytes>() {
        out.push(TAG_BYTES);
        write_len(out, bytes.as_bytes().len())?;
        out.extend_from_slice(bytes.as_bytes());
    } else if let Ok(b) = obj.extract::<bool>() {
        out.push(if b { TAG_TRUE } else { TAG_FALSE });
    } else if let Ok(i) = obj.extract::<i64>() {
        out.push(TAG_INT);
        out.extend_from_slice(&i.to_le_bytes());
    } else if let Ok(float) = obj.cast::<PyFloat>() {
        out.push(TAG_FLOAT);
        out.extend_from_slice(&float.value().to_le_bytes());
    } else if let Ok(s) = obj.extract::<String>() {
        out.push(TAG_STR);
        write_len(out, s.len())?;
        out.extend_from_slice(s.as_bytes());
    } else if let Ok(list) = obj.cast::<PyList>() {
        out.push(TAG_LIST);
        write_len(out, list.len())?;
        for item in list.iter() {
            encode_structured_value(&item, out)?;
        }
    } else if let Ok(dict) = obj.cast::<PyDict>() {
        out.push(TAG_DICT);
        write_len(out, dict.len())?;
        for (key, value) in dict.iter() {
            let key: String = key
                .extract()
                .map_err(|_| pyo3::exceptions::PyTypeError::new_err("dict keys must be str"))?;
            write_len(out, key.len())?;
            out.extend_from_slice(key.as_bytes());
            encode_structured_value(&value, out)?;
        }
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "unsupported nested value type; expected dict, list, str, bytes, int, float, bool, or None",
        ));
    }
    Ok(())
}

fn decode_structured(py: Python, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    if !bytes.starts_with(STRUCTURED_MAGIC) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "invalid structured value header",
        ));
    }
    let mut cursor = Cursor::new(&bytes[STRUCTURED_MAGIC.len()..]);
    let value = decode_structured_value(py, &mut cursor)?;
    if cursor.remaining() != 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "trailing bytes in structured value",
        ));
    }
    Ok(value)
}

fn decode_structured_value(py: Python, cursor: &mut Cursor<'_>) -> PyResult<Py<PyAny>> {
    match cursor.read_u8()? {
        TAG_NULL => Ok(py.None()),
        TAG_FALSE => Ok(PyBool::new(py, false).into_any().unbind()),
        TAG_TRUE => Ok(PyBool::new(py, true).into_any().unbind()),
        TAG_INT => Ok(i64::from_le_bytes(cursor.read_array::<8>()?)
            .into_pyobject(py)?
            .into_any()
            .unbind()),
        TAG_FLOAT => Ok(f64::from_le_bytes(cursor.read_array::<8>()?)
            .into_pyobject(py)?
            .into_any()
            .unbind()),
        TAG_STR => {
            let len = cursor.read_len()?;
            let bytes = cursor.read_bytes(len)?;
            let text = std::str::from_utf8(bytes).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid structured UTF-8: {e}"))
            })?;
            Ok(text.into_pyobject(py)?.into_any().unbind())
        }
        TAG_BYTES => {
            let len = cursor.read_len()?;
            Ok(PyBytes::new(py, cursor.read_bytes(len)?)
                .into_any()
                .unbind())
        }
        TAG_LIST => {
            let len = cursor.read_len()?;
            let list = PyList::empty(py);
            for _ in 0..len {
                list.append(decode_structured_value(py, cursor)?)?;
            }
            Ok(list.into_any().unbind())
        }
        TAG_DICT => {
            let len = cursor.read_len()?;
            let dict = PyDict::new(py);
            for _ in 0..len {
                let key_len = cursor.read_len()?;
                let key_bytes = cursor.read_bytes(key_len)?;
                let key = std::str::from_utf8(key_bytes).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid structured dict key: {e}"
                    ))
                })?;
                dict.set_item(key, decode_structured_value(py, cursor)?)?;
            }
            Ok(dict.into_any().unbind())
        }
        tag => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown structured value tag: {tag}"
        ))),
    }
}

fn write_len(out: &mut Vec<u8>, len: usize) -> PyResult<()> {
    let len = u32::try_from(len)
        .map_err(|_| pyo3::exceptions::PyOverflowError::new_err("structured value is too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn read_u8(&mut self) -> PyResult<u8> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    fn read_len(&mut self) -> PyResult<usize> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?) as usize)
    }

    fn read_array<const N: usize>(&mut self) -> PyResult<[u8; N]> {
        let bytes = self.read_bytes(N)?;
        Ok(bytes.try_into().expect("slice length checked"))
    }

    fn read_bytes(&mut self, len: usize) -> PyResult<&'a [u8]> {
        let end = self.pos.checked_add(len).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("structured value length overflow")
        })?;
        if end > self.bytes.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "truncated structured value",
            ));
        }
        let bytes = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }
}

/// Convert a Rust `Error` into a Python `RuntimeError`.
pub fn error_to_py(err: crate::error::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
}
