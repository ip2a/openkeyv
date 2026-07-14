use crate::protocol::AsyncKeyValue;
use crate::py::error::error_to_py;
use crate::py::value::{optional_value_to_py, py_to_value};
use pyo3::prelude::*;
use std::sync::Arc;

fn with_gil<F, R>(f: F) -> PyResult<R>
where
    F: for<'py> FnOnce(Python<'py>) -> PyResult<R>,
{
    Python::try_attach(f).unwrap_or_else(|| {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "GIL not available",
        ))
    })
}

// ---------------------------------------------------------------------------
// VaultStore
// ---------------------------------------------------------------------------
#[cfg(feature = "vault")]
#[pyclass(subclass, name = "VaultStore")]
pub struct PyVaultStore {
    inner: Arc<crate::store::vault::VaultStore>,
}

#[cfg(feature = "vault")]
#[pymethods]
impl PyVaultStore {
    #[new]
    #[pyo3(signature = (url, token, mount_point = None))]
    fn new(url: String, token: String, mount_point: Option<String>) -> PyResult<Self> {
        let mount_point = mount_point.as_deref();
        let store =
            crate::store::vault::VaultStore::new(&url, &token, mount_point).map_err(error_to_py)?;
        Ok(Self {
            inner: Arc::new(store),
        })
    }

    #[pyo3(signature = (key, collection = None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: String,
        collection: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .get(&key, collection.as_deref())
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let obj = optional_value_to_py(py, result)?;
                Ok(obj)
            })
        })
    }

    #[pyo3(signature = (key, collection = None))]
    fn ttl<'py>(
        &self,
        py: Python<'py>,
        key: String,
        collection: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .ttl(&key, collection.as_deref())
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let tuple = match result {
                    Some((value, ttl)) => {
                        let dict = optional_value_to_py(py, Some(value))?;
                        (dict, ttl).into_pyobject(py)?.to_owned().unbind()
                    }
                    None => (py.None(), py.None())
                        .into_pyobject(py)?
                        .to_owned()
                        .unbind(),
                };
                Ok(tuple)
            })
        })
    }

    #[pyo3(signature = (key, value, collection = None, ttl = None))]
    fn put<'py>(
        &self,
        py: Python<'py>,
        key: String,
        value: Bound<'py, PyAny>,
        collection: Option<String>,
        ttl: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        let value = with_gil(|_py| py_to_value(&value))?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store
                .put(&key, value, collection.as_deref(), ttl)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }

    #[pyo3(signature = (key, collection = None))]
    fn delete<'py>(
        &self,
        py: Python<'py>,
        key: String,
        collection: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .delete(&key, collection.as_deref())
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    #[pyo3(signature = (keys, collection = None))]
    fn get_many<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        collection: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let results = store
                .get_many(&keys, collection.as_deref())
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::empty(py);
                for item in results {
                    list.append(optional_value_to_py(py, item)?)?;
                }
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (keys, collection = None))]
    fn ttl_many<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        collection: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let results = store
                .ttl_many(&keys, collection.as_deref())
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::empty(py);
                for item in results {
                    let tuple = match item {
                        Some((value, ttl)) => {
                            let dict = optional_value_to_py(py, Some(value))?;
                            (dict, ttl).into_pyobject(py)?.to_owned().unbind()
                        }
                        None => (py.None(), py.None())
                            .into_pyobject(py)?
                            .to_owned()
                            .unbind(),
                    };
                    list.append(tuple)?;
                }
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (keys, values, collection = None, ttl = None))]
    fn put_many<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        values: Vec<Bound<'py, PyAny>>,
        collection: Option<String>,
        ttl: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        let values: Vec<_> = values
            .iter()
            .map(|v| py_to_value(v))
            .collect::<PyResult<_>>()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store
                .put_many(&keys, &values, collection.as_deref(), ttl)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }

    #[pyo3(signature = (keys, collection = None))]
    fn delete_many<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        collection: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = store
                .delete_many(&keys, collection.as_deref())
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(count.into_pyobject(py)?.to_owned().unbind()))
        })
    }
}
