use {
    futures::{
        future::{BoxFuture, FutureExt},
        task::{waker_ref, ArcWake},
    },
    once_cell::sync::OnceCell,
    pyo3::{
        callback::IntoPyCallbackOutput, ffi, iter::IterNextOutput, prelude::*, types::IntoPyDict,
        PyAsyncProtocol, PyIterProtocol,
    },
    std::{
        future::Future,
        marker::PhantomData,
        sync::Arc,
        task::{Context, Poll},
    },
};

fn asyncio(py: Python) -> PyResult<&Py<PyModule>> {
    static ASYNCIO: OnceCell<Py<PyModule>> = OnceCell::new();
    ASYNCIO.get_or_try_init(|| Ok(PyModule::import(py, "asyncio")?.into()))
}

fn register_task(py: Python, task: PyObject) -> PyResult<()> {
    static REGISTER_TASK: OnceCell<PyObject> = OnceCell::new();
    REGISTER_TASK
        .get_or_try_init::<_, PyErr>(|| Ok(asyncio(py)?.getattr(py, "_register_task")?))?
        .call1(py, (task,))?;
    Ok(())
}

fn enter_task(py: Python, loop_: PyObject, task: PyObject) -> PyResult<()> {
    static ENTER_TASK: OnceCell<PyObject> = OnceCell::new();
    ENTER_TASK
        .get_or_try_init::<_, PyErr>(|| Ok(asyncio(py)?.getattr(py, "_enter_task")?))?
        .call1(py, (loop_, task))?;
    Ok(())
}

fn leave_task(py: Python, loop_: PyObject, task: PyObject) -> PyResult<()> {
    static LEAVE_TASK: OnceCell<PyObject> = OnceCell::new();
    LEAVE_TASK
        .get_or_try_init::<_, PyErr>(|| Ok(asyncio(py)?.getattr(py, "_leave_task")?))?
        .call1(py, (loop_, task))?;
    Ok(())
}

fn get_running_loop(py: Python) -> PyResult<PyObject> {
    static GET_RUNNING_LOOP: OnceCell<PyObject> = OnceCell::new();
    Ok(GET_RUNNING_LOOP
        .get_or_try_init::<_, PyErr>(|| Ok(asyncio(py)?.getattr(py, "get_running_loop")?))?
        .call0(py)?
        .into())
}

#[pyclass]
struct AwaitableRustFuture {
    future: Option<BoxFuture<'static, PyResult<PyObject>>>,
    aio_loop: Option<PyObject>,
    callbacks: Vec<(PyObject, Option<PyObject>)>,
    #[pyo3(get, set)]
    _asyncio_future_blocking: bool,
    waker: Option<Arc<AsyncioWaker>>,
}

impl AwaitableRustFuture {
    fn new(future: impl Into<BoxFuture<'static, PyResult<PyObject>>>) -> PyResult<Self> {
        Ok(Self {
            future: Some(future.into()),
            aio_loop: None,
            callbacks: vec![],
            _asyncio_future_blocking: true,
            waker: None,
        })
    }
}

#[pyproto]
impl PyAsyncProtocol for AwaitableRustFuture {
    fn __await__(slf: Py<Self>) -> PyResult<Py<Self>> {
        let wrapper = slf.clone();
        Python::with_gil(|py| -> PyResult<_> {
            let mut slf = slf.try_borrow_mut(py)?;
            let aio_loop = get_running_loop(py)?;
            slf.aio_loop = Some(aio_loop.clone());
            slf.waker = Some(Arc::new(AsyncioWaker { aio_loop, wrapper }));
            Ok(())
        })?;

        Ok(slf)
    }
}

#[pyproto]
impl PyIterProtocol for AwaitableRustFuture {
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
struct AsyncioWaker {
    aio_loop: PyObject,
    wrapper: Py<AwaitableRustFuture>,
}

impl ArcWake for AsyncioWaker {
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
impl AsyncioWaker {
    #[call]
    fn __call__(slf: PyRef<Self>) -> PyResult<()> {
        let py = slf.py();
        let mut wrapper = slf.wrapper.try_borrow_mut(py)?;
        if wrapper.callbacks.is_empty() {
            panic!("nothing to call back")
        }
        let callbacks = std::mem::take(&mut wrapper.callbacks);
        for (callback, context) in callbacks {
            slf.aio_loop.call_method(
                py,
                "call_soon",
                (callback, &wrapper),
                Some(vec![("context", context)].into_py_dict(py)),
            )?;
        }
        Ok(())
    }
}

#[pymethods]
impl AwaitableRustFuture {
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

struct PySendableFuture(BoxFuture<'static, PyResult<PyObject>>);

impl<TFuture, TOutput> From<TFuture> for PySendableFuture
where
    TFuture: Future<Output = TOutput> + Send + 'static,
    TOutput: IntoPyCallbackOutput<PyObject>,
{
    fn from(future: TFuture) -> Self {
        Self(
            async move {
                let result = future.await;
                Python::with_gil(move |py| result.convert(py))
            }
            .boxed(),
        )
    }
}

impl From<PySendableFuture> for BoxFuture<'static, PyResult<PyObject>> {
    fn from(wrapper: PySendableFuture) -> Self {
        wrapper.0
    }
}

pub struct PyAsync<T>(PySendableFuture, PhantomData<T>);

impl<TFuture, TOutput> From<TFuture> for PyAsync<TOutput>
where
    TFuture: Future<Output = TOutput> + Send + 'static,
    TOutput: IntoPyCallbackOutput<PyObject>,
{
    fn from(future: TFuture) -> Self {
        Self(future.into(), PhantomData)
    }
}

impl<TOutput> IntoPyCallbackOutput<*mut ffi::PyObject> for PyAsync<TOutput>
where
    TOutput: IntoPyCallbackOutput<PyObject>,
{
    fn convert(self, py: Python) -> PyResult<*mut ffi::PyObject> {
        AwaitableRustFuture::new(self.0).convert(py)
    }
}
