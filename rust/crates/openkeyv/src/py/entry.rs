use crate::ManagedEntry;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::value::{py_to_value, value_to_py};

#[pyfunction]
#[pyo3(name = "_encode_entry")]
fn encode_entry<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
    created_at_millis: Option<i64>,
    expires_at_millis: Option<i64>,
) -> PyResult<Bound<'py, PyBytes>> {
    let created_at = match created_at_millis {
        Some(millis) => Some(Utc.timestamp_millis_opt(millis).single().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "invalid created_at timestamp milliseconds: {millis}"
            ))
        })?),
        None => None,
    };
    let expires_at = match expires_at_millis {
        Some(millis) => Some(Utc.timestamp_millis_opt(millis).single().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "invalid expires_at timestamp milliseconds: {millis}"
            ))
        })?),
        None => None,
    };

    let entry = ManagedEntry {
        value: py_to_value(value)?,
        created_at,
        expires_at,
    };

    Ok(PyBytes::new(py, &entry.encode()))
}

#[pyfunction]
#[pyo3(name = "_decode_entry")]
fn decode_entry(py: Python<'_>, encoded: &[u8]) -> PyResult<(Py<PyAny>, Option<i64>, Option<i64>)> {
    let entry = ManagedEntry::decode(Bytes::copy_from_slice(encoded))
        .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
    let value = value_to_py(py, &entry.value)?;
    let created_at_millis = entry.created_at.map(|value| value.timestamp_millis());
    let expires_at_millis = entry.expires_at.map(|value| value.timestamp_millis());

    Ok((value, created_at_millis, expires_at_millis))
}

#[pyfunction]
#[pyo3(name = "_prepare_entry_timestamps")]
fn prepare_entry_timestamps(ttl: Option<f64>) -> PyResult<(i64, Option<i64>)> {
    let entry = match ttl {
        Some(ttl) => ManagedEntry::with_ttl(crate::Value::null(), ttl),
        None => Ok(ManagedEntry::new(crate::Value::null())),
    }
    .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;

    Ok((
        entry
            .created_at
            .expect("new entries have creation timestamps")
            .timestamp_millis(),
        entry.expires_at.map(|value| value.timestamp_millis()),
    ))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_entry, m)?)?;
    m.add_function(wrap_pyfunction!(decode_entry, m)?)?;
    m.add_function(wrap_pyfunction!(prepare_entry_timestamps, m)?)?;
    Ok(())
}
