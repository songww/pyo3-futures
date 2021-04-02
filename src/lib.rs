use {
    futures::{
        future::{BoxFuture, FutureExt},
        task::{waker_ref, ArcWake},
    },
    once_cell::sync::OnceCell,
    pyo3::{
        callback::IntoPyCallbackOutput, ffi, iter::IterNextOutput, prelude::*,
        type_object::PyTypeObject, types::IntoPyDict, PyAsyncProtocol, PyIterProtocol,
    },
    std::{
        future::Future,
        marker::PhantomData,
        mem,
        sync::Arc,
        task::{Context, Poll},
    },
};

fn monkey_patch_ourselves_into_accepted_coro_types(py: Python) -> PyResult<&()> {
    static MONKEY_PATCHED: OnceCell<()> = OnceCell::new();
    MONKEY_PATCHED.get_or_try_init(|| {
        let coroutines = PyModule::import(py, "asyncio.coroutines")?;
        let typecache_set: &pyo3::types::PySet =
            coroutines.getattr("_iscoroutine_typecache")?.extract()?;
        typecache_set.add(RustTask::type_object(py))?;
        Ok(())
    })
}

fn asyncio(py: Python) -> PyResult<&Py<PyModule>> {
    static ASYNCIO: OnceCell<Py<PyModule>> = OnceCell::new();
    ASYNCIO.get_or_try_init(|| Ok(PyModule::import(py, "asyncio")?.into()))
}

fn get_running_loop(py: Python) -> PyResult<PyObject> {
    static GET_RUNNING_LOOP: OnceCell<PyObject> = OnceCell::new();
    Ok(GET_RUNNING_LOOP
        .get_or_try_init::<_, PyErr>(|| Ok(asyncio(py)?.getattr(py, "get_running_loop")?))?
        .call0(py)?
        .into())
}

enum FutureState {
    Cancelled,
    Pending {
        future: BoxFuture<'static, PyResult<PyObject>>,
        waker: Arc<AsyncioWaker>,
    },
    Executing,
}

#[pyclass(weakref)]
struct RustTask {
    future: FutureState,
    aio_loop: PyObject,
    callbacks: Vec<(PyObject, Option<PyObject>)>,
    #[pyo3(get, set)]
    _asyncio_future_blocking: bool,
}

#[pyproto]
impl PyAsyncProtocol for RustTask {
    fn __await__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
}

#[pyproto]
impl PyIterProtocol for RustTask {
    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<IterNextOutput<PyRefMut<Self>, PyObject>> {
        let mut execution_slot = FutureState::Executing;
        mem::swap(&mut execution_slot, &mut slf.future);
        let result = match &mut execution_slot {
            FutureState::Pending { future, waker } => slf.py().allow_threads(|| {
                let waker_ref = waker_ref(&waker);
                let context = &mut Context::from_waker(&*waker_ref);
                future.as_mut().poll(context)
            }),
            _ => unimplemented!(),
        };
        mem::swap(&mut execution_slot, &mut slf.future);
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
    wrapper: Py<RustTask>,
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
        let mut wrapper = slf.wrapper.try_borrow_mut(py)?; // TODO: Try moving the callbacks to the waker
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
impl RustTask {
    fn get_loop(&self) -> &PyObject {
        &self.aio_loop
    }

    fn add_done_callback(&mut self, callback: PyObject, context: Option<PyObject>) {
        self.callbacks.push((callback, context));
    }

    /// https://docs.python.org/3/reference/datamodel.html#coroutine.send
    fn send(
        slf: PyRefMut<Self>,
        value: Option<&PyAny>,
    ) -> PyResult<IterNextOutput<PyObject, PyObject>> {
        if value.is_some() {
            unimplemented!();
        }
        Python::with_gil(|py| Self::__next__(slf).convert(py))
    }

    /// https://docs.python.org/3/reference/datamodel.html#coroutine.throw
    fn throw(slf: Py<Self>, type_: &PyAny, exc: Option<&PyAny>, traceback: Option<&PyAny>) {
        panic!("throw({:?}, {:?}, {:?})", type_, exc, traceback);
    }

    fn result(&self) {}
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
        monkey_patch_ourselves_into_accepted_coro_types(py)?;
        let aio_loop = get_running_loop(py)?;
        let task_like = Py::new(
            py,
            RustTask {
                future: FutureState::Cancelled,
                aio_loop: aio_loop.clone(),
                callbacks: vec![],
                _asyncio_future_blocking: false,
            },
        )?;
        let wrapper = task_like.clone();
        task_like.try_borrow_mut(py)?.future = FutureState::Pending {
            future: self.0.into(),
            waker: Arc::new(AsyncioWaker {
                aio_loop: aio_loop,
                wrapper,
            }),
        };
        task_like.convert(py)
    }
}
