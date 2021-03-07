use {
    futures::{
        future::{BoxFuture, FutureExt},
        task::{waker_ref, ArcWake},
    },
    pyo3::{
        callback::IntoPyCallbackOutput, iter::IterNextOutput, prelude::*, types::IntoPyDict,
        PyAsyncProtocol, PyIterProtocol,
    },
    std::{
        future::Future,
        sync::Arc,
        task::{Context, Poll},
    },
};

#[pyclass]
pub struct RustFutureWrapper {
    future: Option<BoxFuture<'static, PyResult<PyObject>>>,
    aio_loop: Option<PyObject>,
    callbacks: Vec<(PyObject, Option<PyObject>)>,
    #[pyo3(get, set)]
    _asyncio_future_blocking: bool,
    waker: Option<Arc<WakerClosure>>,
}

impl RustFutureWrapper {
    fn new(
        future: impl Future<Output = impl IntoPyCallbackOutput<PyObject>> + Send + 'static,
    ) -> Self {
        Self {
            future: Some(
                async move {
                    let result = future.await;
                    Python::with_gil(move |py| result.convert(py))
                }
                .boxed(),
            ),
            aio_loop: None,
            callbacks: vec![],
            _asyncio_future_blocking: true,
            waker: None,
        }
    }
}

#[pyproto]
impl PyAsyncProtocol for RustFutureWrapper {
    fn __await__(slf: Py<Self>) -> PyResult<Py<Self>> {
        let py_future = slf.clone();
        Python::with_gil(|py| -> PyResult<_> {
            let mut slf = slf.try_borrow_mut(py)?;
            let aio_loop: PyObject = PyModule::import(py, "asyncio")?
                .call0("get_running_loop")?
                .into();
            slf.aio_loop = Some(aio_loop.clone());
            slf.waker = Some(Arc::new(WakerClosure {
                aio_loop,
                py_future,
            }));
            Ok(())
        })?;

        Ok(slf)
    }
}

#[pyproto]
impl PyIterProtocol for RustFutureWrapper {
    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<IterNextOutput<PyRefMut<Self>, PyObject>> {
        let mut future = slf.future.take().expect("no future");
        let waker = slf.waker.take().expect("no waker");
        let result = slf.py().allow_threads(|| {
            let waker_ref = waker_ref(&waker);
            let context = &mut Context::from_waker(&*waker_ref);
            future.as_mut().poll(context)
        });
        slf.future = Some(future);
        slf.waker = Some(waker);
        match result {
            Poll::Pending => {
                slf._asyncio_future_blocking = true;
                Ok(IterNextOutput::Yield(slf))
            }
            Poll::Ready(result) => Ok(IterNextOutput::Return(result?)),
        }
    }
}

#[pyclass]
#[derive(Clone)]
struct WakerClosure {
    aio_loop: PyObject,
    py_future: Py<RustFutureWrapper>,
}

impl ArcWake for WakerClosure {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let closure = (**arc_self).clone();
        Python::with_gil(|py| {
            arc_self
                .aio_loop
                .call_method1(py, "call_soon_threadsafe", (closure,))
        })
        .expect("exception thrown by the event loop (probably closed)");
    }
}

#[pymethods]
impl WakerClosure {
    #[call]
    pub fn __call__(slf: PyRef<Self>) -> PyResult<()> {
        let py = slf.py();
        let mut py_future = slf.py_future.try_borrow_mut(py)?;
        if py_future.callbacks.is_empty() {
            panic!("nothing to call back")
        }
        let callbacks = std::mem::take(&mut py_future.callbacks);
        for (callback, context) in callbacks {
            slf.aio_loop.call_method(
                py,
                "call_soon",
                (callback, &py_future),
                Some(vec![("context", context)].into_py_dict(py)),
            )?;
        }
        Ok(())
    }
}

#[pymethods]
impl RustFutureWrapper {
    fn get_loop(&self) -> Option<&PyObject> {
        self.aio_loop.as_ref()
    }

    fn add_done_callback(&mut self, callback: PyObject, context: Option<PyObject>) {
        self.callbacks.push((callback, context));
    }

    fn result(&self) -> Option<PyObject> {
        None
    }
}

pub trait PyAwaitable:
    IntoPyCallbackOutput<*mut pyo3::ffi::PyObject> + PyAsyncProtocol<'static> + PyIterProtocol<'static>
{
}

impl<T> PyAwaitable for T where
    T: IntoPyCallbackOutput<*mut pyo3::ffi::PyObject>
        + PyAsyncProtocol<'static>
        + PyIterProtocol<'static>
{
}

pub trait IntoPyAwaitable<'p> {
    type Result: PyAwaitable;
    fn into_awaitable(self) -> Self::Result;
}

impl<T: Future<Output = impl IntoPyCallbackOutput<PyObject>> + Send + 'static> IntoPyAwaitable<'_>
    for T
{
    type Result = RustFutureWrapper;
    fn into_awaitable(self) -> Self::Result {
        RustFutureWrapper::new(self)
    }
}
