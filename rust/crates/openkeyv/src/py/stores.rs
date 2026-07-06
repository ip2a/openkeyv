use super::utils::*;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
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
// MemoryStore
// ---------------------------------------------------------------------------
#[pyclass(subclass, name = "MemoryStore")]
pub struct PyMemoryStore {
    inner: Arc<crate::store::memory::MemoryStore>,
}

#[pymethods]
impl PyMemoryStore {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(crate::store::memory::MemoryStore::new()),
        }
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// SimpleStore
// ---------------------------------------------------------------------------
#[pyclass(subclass, name = "SimpleStore")]
pub struct PySimpleStore {
    inner: Arc<crate::store::simple::SimpleStore>,
}

#[pymethods]
impl PySimpleStore {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(crate::store::simple::SimpleStore::new()),
        }
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
                let obj = hashmap_to_py(py, result)?;
                Ok(obj)
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// FileTreeStore
// ---------------------------------------------------------------------------
#[pyclass(subclass, name = "FileTreeStore")]
pub struct PyFileTreeStore {
    inner: Arc<crate::store::filetree::store::FileTreeStore>,
}

#[pymethods]
impl PyFileTreeStore {
    #[new]
    fn new(base_path: String) -> Self {
        Self {
            inner: Arc::new(crate::store::filetree::store::FileTreeStore::new(base_path)),
        }
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
                let obj = hashmap_to_py(py, result)?;
                Ok(obj)
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// NullStore
// ---------------------------------------------------------------------------
#[pyclass(subclass, name = "NullStore")]
pub struct PyNullStore {
    inner: Arc<crate::store::null::NullStore>,
}

#[pymethods]
impl PyNullStore {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(crate::store::null::NullStore::new()),
        }
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
                let obj = hashmap_to_py(py, result)?;
                Ok(obj)
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store
                .put(&key, value, collection.as_deref(), ttl)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// DiskStore (feature-gated)
// ---------------------------------------------------------------------------
#[cfg(feature = "disk")]
#[pyclass(subclass, name = "DiskStore")]
pub struct PyDiskStore {
    inner: Arc<crate::store::disk::DiskStore>,
}

#[cfg(feature = "disk")]
#[pymethods]
impl PyDiskStore {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        let store = crate::store::disk::DiskStore::new(&path).map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
                Ok(obj)
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// RedisStore
// ---------------------------------------------------------------------------
#[cfg(feature = "redis")]
#[pyclass(subclass, name = "RedisStore")]
pub struct PyRedisStore {
    inner: Arc<crate::store::redis::RedisStore>,
}

#[cfg(feature = "redis")]
#[pymethods]
impl PyRedisStore {
    #[new]
    #[pyo3(signature = (url))]
    fn new(url: String) -> PyResult<Self> {
        let store = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?
            .block_on(async { crate::store::redis::RedisStore::new(&url).await })
            .map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// ValkeyStore
// ---------------------------------------------------------------------------
#[cfg(feature = "valkey")]
#[pyclass(subclass, name = "ValkeyStore")]
pub struct PyValkeyStore {
    inner: Arc<crate::store::valkey::ValkeyStore>,
}

#[cfg(feature = "valkey")]
#[pymethods]
impl PyValkeyStore {
    #[new]
    #[pyo3(signature = (url))]
    fn new(url: String) -> PyResult<Self> {
        let store = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?
            .block_on(async { crate::store::valkey::ValkeyStore::new(&url).await })
            .map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// RocksDBStore
// ---------------------------------------------------------------------------
#[cfg(feature = "rocksdb")]
#[pyclass(subclass, name = "RocksDBStore")]
pub struct PyRocksDBStore {
    inner: Arc<crate::store::rocksdb::RocksDBStore>,
}

#[cfg(feature = "rocksdb")]
#[pymethods]
impl PyRocksDBStore {
    #[new]
    #[pyo3(signature = (path))]
    fn new(path: String) -> PyResult<Self> {
        let store = crate::store::rocksdb::RocksDBStore::new(&path).map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// PostgresStore
// ---------------------------------------------------------------------------
#[cfg(feature = "postgres")]
#[pyclass(subclass, name = "PostgresStore")]
pub struct PyPostgresStore {
    inner: Arc<crate::store::postgres::PostgresStore>,
}

#[cfg(feature = "postgres")]
#[pymethods]
impl PyPostgresStore {
    #[new]
    #[pyo3(signature = (url, table_name = None))]
    fn new(url: String, table_name: Option<String>) -> PyResult<Self> {
        let table_name = table_name.as_deref();
        let store = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?
            .block_on(async { crate::store::postgres::PostgresStore::new(&url, table_name).await })
            .map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// MongoStore
// ---------------------------------------------------------------------------
#[cfg(feature = "mongodb")]
#[pyclass(subclass, name = "MongoStore")]
pub struct PyMongoStore {
    inner: Arc<crate::store::mongodb::MongoStore>,
}

#[cfg(feature = "mongodb")]
#[pymethods]
impl PyMongoStore {
    #[new]
    #[pyo3(signature = (url))]
    fn new(url: String) -> PyResult<Self> {
        let store = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?
            .block_on(async { crate::store::mongodb::MongoStore::new(&url).await })
            .map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// DynamoDBStore
// ---------------------------------------------------------------------------
#[cfg(feature = "dynamodb")]
#[pyclass(subclass, name = "DynamoDBStore")]
pub struct PyDynamoDBStore {
    inner: Arc<crate::store::dynamodb::DynamoDBStore>,
}

#[cfg(feature = "dynamodb")]
#[pymethods]
impl PyDynamoDBStore {
    #[new]
    #[pyo3(signature = (table_name))]
    fn new(table_name: String) -> PyResult<Self> {
        let store = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?
            .block_on(async { crate::store::dynamodb::DynamoDBStore::new(&table_name).await })
            .map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// S3Store
// ---------------------------------------------------------------------------
#[cfg(feature = "s3")]
#[pyclass(subclass, name = "S3Store")]
pub struct PyS3Store {
    inner: Arc<crate::store::s3::S3Store>,
}

#[cfg(feature = "s3")]
#[pymethods]
impl PyS3Store {
    #[new]
    #[pyo3(signature = (bucket_name))]
    fn new(bucket_name: String) -> PyResult<Self> {
        let store = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?
            .block_on(async { crate::store::s3::S3Store::new(&bucket_name).await })
            .map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// DuckDBStore
// ---------------------------------------------------------------------------
#[cfg(feature = "duckdb-store")]
#[pyclass(subclass, name = "DuckDBStore")]
pub struct PyDuckDBStore {
    inner: Arc<crate::store::duckdb::DuckDBStore>,
}

#[cfg(feature = "duckdb-store")]
#[pymethods]
impl PyDuckDBStore {
    #[new]
    #[pyo3(signature = (path = None, table_name = None))]
    fn new(path: Option<String>, table_name: Option<String>) -> PyResult<Self> {
        let path = path.as_deref();
        let table_name = table_name.as_deref();
        let store = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?
            .block_on(async { crate::store::duckdb::DuckDBStore::new(path, table_name).await })
            .map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}

// ---------------------------------------------------------------------------
// MemcachedStore
// ---------------------------------------------------------------------------
#[cfg(feature = "memcached")]
#[pyclass(subclass, name = "MemcachedStore")]
pub struct PyMemcachedStore {
    inner: Arc<crate::store::memcached::MemcachedStore>,
}

#[cfg(feature = "memcached")]
#[pymethods]
impl PyMemcachedStore {
    #[new]
    #[pyo3(signature = (url))]
    fn new(url: String) -> PyResult<Self> {
        let store = crate::store::memcached::MemcachedStore::new(&url).map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

// ---------------------------------------------------------------------------
// KeyringStore
// ---------------------------------------------------------------------------
#[cfg(feature = "keyring-store")]
#[pyclass(subclass, name = "KeyringStore")]
pub struct PyKeyringStore {
    inner: Arc<crate::store::keyring::KeyringStore>,
}

#[cfg(feature = "keyring-store")]
#[pymethods]
impl PyKeyringStore {
    #[new]
    #[pyo3(signature = (service_name = None))]
    fn new(service_name: Option<String>) -> PyResult<Self> {
        let service_name = service_name.as_deref();
        let store = crate::store::keyring::KeyringStore::new(service_name);
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

// ---------------------------------------------------------------------------
// FirestoreStore
// ---------------------------------------------------------------------------
#[cfg(feature = "firestore-store")]
#[pyclass(subclass, name = "FirestoreStore")]
pub struct PyFirestoreStore {
    inner: Arc<crate::store::firestore::FirestoreStore>,
}

#[cfg(feature = "firestore-store")]
#[pymethods]
impl PyFirestoreStore {
    #[new]
    #[pyo3(signature = (project_id))]
    fn new(project_id: String) -> PyResult<Self> {
        let store = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?
            .block_on(async { crate::store::firestore::FirestoreStore::new(&project_id).await })
            .map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

// ---------------------------------------------------------------------------
// OpenSearchStore
// ---------------------------------------------------------------------------
#[cfg(feature = "opensearch")]
#[pyclass(subclass, name = "OpenSearchStore")]
pub struct PyOpenSearchStore {
    inner: Arc<crate::store::opensearch::OpenSearchStore>,
}

#[cfg(feature = "opensearch")]
#[pymethods]
impl PyOpenSearchStore {
    #[new]
    #[pyo3(signature = (url, index_prefix))]
    fn new(url: String, index_prefix: String) -> PyResult<Self> {
        let store = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?
            .block_on(async {
                crate::store::opensearch::OpenSearchStore::from_url(&url, &index_prefix).await
            })
            .map_err(error_to_py)?;
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
                let obj = hashmap_to_py(py, result)?;
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
                        let dict = hashmap_to_py(py, Some(value))?;
                        (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
        let value = with_gil(|_py| py_to_hashmap(&value))?;
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
                    list.append(hashmap_to_py(py, item)?)?;
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
                            let dict = hashmap_to_py(py, Some(value))?;
                            (dict, Some(ttl)).into_pyobject(py)?.to_owned().unbind()
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
            .map(|v| py_to_hashmap(v))
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

    #[pyo3(signature = (collection = None, limit = None))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        collection: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .keys(collection.as_deref(), limit)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (limit = None))]
    fn collections<'py>(
        &self,
        py: Python<'py>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.collections(limit).await.map_err(error_to_py)?;
            with_gil(|py| {
                let list = pyo3::types::PyList::new(py, result)?;
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (collection))]
    fn destroy_collection<'py>(
        &self,
        py: Python<'py>,
        collection: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .destroy_collection(&collection)
                .await
                .map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn destroy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.destroy().await.map_err(error_to_py)?;
            with_gil(|py| Ok(result.into_pyobject(py)?.to_owned().unbind()))
        })
    }

    fn cull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.cull().await.map_err(error_to_py)?;
            with_gil(|py| Ok(py.None()))
        })
    }
}
// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyMemoryStore>()?;
    m.add_class::<PySimpleStore>()?;
    m.add_class::<PyFileTreeStore>()?;
    m.add_class::<PyNullStore>()?;
    #[cfg(feature = "disk")]
    m.add_class::<PyDiskStore>()?;
    #[cfg(feature = "redis")]
    m.add_class::<PyRedisStore>()?;
    #[cfg(feature = "valkey")]
    m.add_class::<PyValkeyStore>()?;
    #[cfg(feature = "rocksdb")]
    m.add_class::<PyRocksDBStore>()?;
    #[cfg(feature = "postgres")]
    m.add_class::<PyPostgresStore>()?;
    #[cfg(feature = "mongodb")]
    m.add_class::<PyMongoStore>()?;
    #[cfg(feature = "dynamodb")]
    m.add_class::<PyDynamoDBStore>()?;
    #[cfg(feature = "s3")]
    m.add_class::<PyS3Store>()?;
    #[cfg(feature = "duckdb-store")]
    m.add_class::<PyDuckDBStore>()?;
    #[cfg(feature = "memcached")]
    m.add_class::<PyMemcachedStore>()?;
    #[cfg(feature = "vault")]
    m.add_class::<PyVaultStore>()?;
    #[cfg(feature = "keyring-store")]
    m.add_class::<PyKeyringStore>()?;
    #[cfg(feature = "firestore-store")]
    m.add_class::<PyFirestoreStore>()?;
    #[cfg(feature = "opensearch")]
    m.add_class::<PyOpenSearchStore>()?;
    Ok(())
}
