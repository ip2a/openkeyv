pub mod entry;
pub mod error;
pub mod stores;
pub mod value;

use pyo3::prelude::*;

#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    entry::register(m)?;
    stores::register(m)?;
    Ok(())
}
