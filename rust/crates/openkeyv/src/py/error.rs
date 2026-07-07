/// Convert a Rust `Error` into a Python `RuntimeError`.
pub fn error_to_py(err: crate::error::Error) -> pyo3::PyErr {
    pyo3::PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
}
