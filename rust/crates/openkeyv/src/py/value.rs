use crate::value::{StructuredValue, Value, ValueKind};
use bytes::Bytes;
use pyo3::BoundObject;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBoolMethods, PyBytes, PyDict, PyFloat, PyInt, PyList};

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
        ValueKind::UnsignedInteger => {
            let bytes = fixed_bytes::<8>(value.bytes(), "unsigned integer")?;
            Ok(u64::from_le_bytes(bytes)
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
        ValueKind::Structured => structured_to_py(
            py,
            &StructuredValue::decode(value.bytes()).map_err(value_error_to_py)?,
        ),
    }
}

pub fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        Ok(Value::null())
    } else if let Ok(bytes) = obj.cast::<PyBytes>() {
        Ok(Value::binary(bytes.as_bytes().to_vec()))
    } else if let Ok(value) = obj.cast::<PyBool>() {
        Ok(Value::bool(value.is_true()))
    } else if let Ok(value) = obj.cast::<PyInt>() {
        if let Ok(value) = value.extract::<i64>() {
            Ok(Value::integer(value))
        } else if let Ok(value) = value.extract::<u64>() {
            Ok(Value::unsigned_integer(value))
        } else {
            Err(pyo3::exceptions::PyOverflowError::new_err(
                "Python int is outside the supported i64::MIN..=u64::MAX range",
            ))
        }
    } else if let Ok(float) = obj.cast::<PyFloat>() {
        Ok(Value::float(float.value()))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(Value::utf8(s))
    } else if obj.cast::<PyList>().is_ok() || obj.cast::<PyDict>().is_ok() {
        let structured = structured_from_py(obj)?;
        Ok(Value::structured(
            structured.encode().map_err(value_error_to_py)?,
        ))
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

fn structured_from_py(obj: &Bound<'_, PyAny>) -> PyResult<StructuredValue> {
    if obj.is_none() {
        Ok(StructuredValue::Null)
    } else if let Ok(bytes) = obj.cast::<PyBytes>() {
        Ok(StructuredValue::Bytes(Bytes::copy_from_slice(
            bytes.as_bytes(),
        )))
    } else if let Ok(value) = obj.cast::<PyBool>() {
        Ok(StructuredValue::Bool(value.is_true()))
    } else if let Ok(value) = obj.cast::<PyInt>() {
        if let Ok(value) = value.extract::<i64>() {
            Ok(StructuredValue::Integer(value))
        } else if let Ok(value) = value.extract::<u64>() {
            Ok(StructuredValue::UnsignedInteger(value))
        } else {
            Err(pyo3::exceptions::PyOverflowError::new_err(
                "Python int is outside the supported i64::MIN..=u64::MAX range",
            ))
        }
    } else if let Ok(float) = obj.cast::<PyFloat>() {
        Ok(StructuredValue::Float(float.value()))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(StructuredValue::String(s))
    } else if let Ok(list) = obj.cast::<PyList>() {
        let mut values = Vec::with_capacity(list.len());
        for item in list.iter() {
            values.push(structured_from_py(&item)?);
        }
        Ok(StructuredValue::List(values))
    } else if let Ok(dict) = obj.cast::<PyDict>() {
        let mut entries = Vec::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let key: String = key
                .extract()
                .map_err(|_| pyo3::exceptions::PyTypeError::new_err("dict keys must be str"))?;
            entries.push((key, structured_from_py(&value)?));
        }
        Ok(StructuredValue::Dict(entries))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "unsupported nested value type; expected dict, list, str, bytes, int, float, bool, or None",
        ))
    }
}

fn structured_to_py(py: Python, value: &StructuredValue) -> PyResult<Py<PyAny>> {
    match value {
        StructuredValue::Null => Ok(py.None()),
        StructuredValue::Bool(false) => Ok(PyBool::new(py, false).into_any().unbind()),
        StructuredValue::Bool(true) => Ok(PyBool::new(py, true).into_any().unbind()),
        StructuredValue::Integer(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        StructuredValue::UnsignedInteger(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        StructuredValue::Float(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        StructuredValue::String(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        StructuredValue::Bytes(value) => Ok(PyBytes::new(py, value).into_any().unbind()),
        StructuredValue::List(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(structured_to_py(py, value)?)?;
            }
            Ok(list.into_any().unbind())
        }
        StructuredValue::Dict(entries) => {
            let dict = PyDict::new(py);
            for (key, value) in entries {
                dict.set_item(key, structured_to_py(py, value)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

fn fixed_bytes<const N: usize>(bytes: &[u8], label: &str) -> PyResult<[u8; N]> {
    bytes.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid {label} payload length"))
    })
}

fn value_error_to_py(err: crate::error::Error) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(err.to_string())
}
