pub mod stores;
pub mod utils;

use pyo3::prelude::*;

#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    stores::register(m)?;
    Ok(())
}
