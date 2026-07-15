use crate::protocol::{CompareAndDeleteResult, CompareAndSwapResult, Revision, RevisionedValue};
use crate::py::value::value_to_py;
use pyo3::prelude::*;
use std::hash::{Hash, Hasher};

/// Opaque revision token returned by stores with atomic conditional-write support.
///
/// Python users cannot construct a revision directly; revisions are only ever
/// produced by store operations and compared for equality.
#[pyclass(name = "Revision")]
pub struct PyRevision {
    pub(crate) inner: Revision,
}

impl PyRevision {
    pub(crate) fn from_rust(rev: Revision) -> Self {
        Self { inner: rev }
    }
}

#[pymethods]
impl PyRevision {
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.extract::<PyRef<'_, PyRevision>>() {
            Ok(other) => self.inner == other.inner,
            Err(_) => false,
        }
    }

    fn __ne__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.extract::<PyRef<'_, PyRevision>>() {
            Ok(other) => self.inner != other.inner,
            Err(_) => true,
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.inner.hash(&mut hasher);
        hasher.finish()
    }

    fn __repr__(&self) -> String {
        "Revision(...)".to_string()
    }
}

/// Value and revision observed from the same atomic store entry.
#[pyclass(name = "RevisionedValue")]
pub struct PyRevisionedValue {
    pub(crate) inner: RevisionedValue,
}

impl PyRevisionedValue {
    pub(crate) fn from_rust(rv: RevisionedValue) -> Self {
        Self { inner: rv }
    }
}

#[pymethods]
impl PyRevisionedValue {
    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        value_to_py(py, &self.inner.value)
    }

    #[getter]
    fn revision(&self) -> PyRevision {
        PyRevision::from_rust(self.inner.revision)
    }

    #[getter]
    fn ttl(&self) -> Option<f64> {
        self.inner.ttl
    }
}

/// Result of an atomic conditional write.
#[pyclass(name = "CompareAndSwapResult")]
pub struct PyCompareAndSwapResult {
    pub(crate) inner: CompareAndSwapResult,
}

impl PyCompareAndSwapResult {
    pub(crate) fn from_rust(result: CompareAndSwapResult) -> Self {
        Self { inner: result }
    }
}

#[pymethods]
impl PyCompareAndSwapResult {
    #[getter]
    fn applied(&self) -> bool {
        matches!(self.inner, CompareAndSwapResult::Applied { .. })
    }

    #[getter]
    fn revision(&self) -> Option<PyRevision> {
        match self.inner {
            CompareAndSwapResult::Applied { revision } => Some(PyRevision::from_rust(revision)),
            CompareAndSwapResult::Conflict { .. } => None,
        }
    }

    #[getter]
    fn current(&self) -> Option<PyRevisionedValue> {
        match &self.inner {
            CompareAndSwapResult::Conflict { current: Some(rv) } => {
                Some(PyRevisionedValue::from_rust(rv.clone()))
            }
            _ => None,
        }
    }
}

/// Result of an atomic conditional delete.
#[pyclass(name = "CompareAndDeleteResult")]
pub struct PyCompareAndDeleteResult {
    pub(crate) inner: CompareAndDeleteResult,
}

impl PyCompareAndDeleteResult {
    pub(crate) fn from_rust(result: CompareAndDeleteResult) -> Self {
        Self { inner: result }
    }
}

#[pymethods]
impl PyCompareAndDeleteResult {
    #[getter]
    fn deleted(&self) -> bool {
        matches!(self.inner, CompareAndDeleteResult::Deleted)
    }

    #[getter]
    fn current(&self) -> Option<PyRevisionedValue> {
        match &self.inner {
            CompareAndDeleteResult::Conflict { current: Some(rv) } => {
                Some(PyRevisionedValue::from_rust(rv.clone()))
            }
            _ => None,
        }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRevision>()?;
    m.add_class::<PyRevisionedValue>()?;
    m.add_class::<PyCompareAndSwapResult>()?;
    m.add_class::<PyCompareAndDeleteResult>()?;
    Ok(())
}
