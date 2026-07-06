use pyo3::BoundObject;
use pyo3::prelude::*;
use pyo3::types::PyBool;
use serde_json::Value;
use std::collections::HashMap;

/// Convert a `serde_json::Value` into a Python object.
pub fn value_to_py(py: Python, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(PyBool::new(py, *b).into_any().unbind()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_pyobject(py)?.into_any().unbind())
            } else {
                let f = n.as_f64().unwrap_or(0.0);
                Ok(f.into_pyobject(py)?.into_any().unbind())
            }
        }
        Value::String(s) => Ok(s.as_str().into_pyobject(py)?.into_any().unbind()),
        Value::Array(arr) => {
            let list = pyo3::types::PyList::empty(py);
            for item in arr {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        Value::Object(obj) => {
            let dict = pyo3::types::PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

/// Convert a Python object into a `serde_json::Value`.
pub fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        Ok(Value::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(Value::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(Value::Number(i.into()))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(Value::Number(
            serde_json::Number::from_f64(f).unwrap_or_else(|| 0.into()),
        ))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(Value::String(s))
    } else if let Ok(list) = obj.cast::<pyo3::types::PyList>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_to_value(&item)?);
        }
        Ok(Value::Array(arr))
    } else if let Ok(dict) = obj.cast::<pyo3::types::PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict {
            let key: String = k.extract()?;
            map.insert(key, py_to_value(&v)?);
        }
        Ok(Value::Object(map))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "unsupported type for JSON serialization",
        ))
    }
}

/// Convert an optional `HashMap<String, Value>` into a Python `dict | None`.
pub fn hashmap_to_py(py: Python, value: Option<HashMap<String, Value>>) -> PyResult<Py<PyAny>> {
    match value {
        Some(map) => {
            let dict = pyo3::types::PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, value_to_py(py, &v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
        None => Ok(py.None()),
    }
}

/// Convert a Python `dict` into a `HashMap<String, Value>`.
pub fn py_to_hashmap(obj: &Bound<'_, PyAny>) -> PyResult<HashMap<String, Value>> {
    let dict = obj.cast::<pyo3::types::PyDict>()?;
    let mut map = HashMap::new();
    for (k, v) in dict {
        let key: String = k.extract()?;
        map.insert(key, py_to_value(&v)?);
    }
    Ok(map)
}

/// Convert a Rust `Error` into a Python `RuntimeError`.
pub fn error_to_py(err: crate::error::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
}
