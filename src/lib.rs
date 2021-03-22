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
        mem,
        sync::Arc,
        task::{Context, Poll},
    },
};

fn asyncio(py: Python) -> PyResult<&Py<PyModule>> {
    static ASYNCIO: OnceCell<Py<PyModule>> = OnceCell::new();
    ASYNCIO.get_or_try_init(|| Ok(PyModule::import(py, "asyncio")?.into()))
}

fn register_task(py: Python, task: &Py<RustTask>) -> PyResult<()> {
    static REGISTER_TASK: OnceCell<PyObject> = OnceCell::new();
    REGISTER_TASK
        .get_or_try_init::<_, PyErr>(|| Ok(asyncio(py)?.getattr(py, "_register_task")?))?
        .call1(py, (task,))?;
    Ok(())
}

fn enter_task(py: Python, loop_: &PyObject, task: &PyRefMut<RustTask>) -> PyResult<()> {
    static ENTER_TASK: OnceCell<PyObject> = OnceCell::new();
    ENTER_TASK
        .get_or_try_init::<_, PyErr>(|| Ok(asyncio(py)?.getattr(py, "_enter_task")?))?
        .call1(py, (loop_, task))?;
    Ok(())
}

fn leave_task(py: Python, loop_: &PyObject, task: &PyRefMut<RustTask>) -> PyResult<()> {
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

enum FutureState {
    Pending {
        future: BoxFuture<'static, PyResult<PyObject>>,
        waker: Arc<AsyncioWaker>,
    },
    Executing,
    Ready(PyResult<PyObject>),
}

impl Default for FutureState {
    fn default() -> Self {
        FutureState::Executing
    }
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
        match slf.result()? {
            None => {
                slf._asyncio_future_blocking = true;
                Ok(IterNextOutput::Yield(slf))
            }
            Some(result) => Ok(IterNextOutput::Return(result)),
        }
    }
}

struct AsyncioWaker {
    aio_loop: PyObject,
    wrapper: Py<RustTask>,
}

impl ArcWake for AsyncioWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        Python::with_gil(|py| {
            arc_self
                .aio_loop
                .call_method1(py, "call_soon_threadsafe", (&arc_self.wrapper,))
        })
        .expect("exception thrown by the event loop (probably closed)");
    }
}

#[pymethods]
impl RustTask {
    /// Equivalent to `asyncio.tasks.Task.__wakeup`
    #[call]
    fn __call__(mut slf: PyRefMut<Self>) -> PyResult<()> {
        enter_task(slf.py(), &slf.aio_loop, &slf)?;
        match mem::take(&mut slf.future) {
            FutureState::Pending { mut future, waker } => {
                let result = slf.py().allow_threads(|| {
                    let waker_ref = waker_ref(&waker);
                    let context = &mut Context::from_waker(&*waker_ref);
                    future.as_mut().poll(context)
                });
                slf.future = match result {
                    Poll::Pending => FutureState::Pending { future, waker },
                    Poll::Ready(result) => {
                        let callbacks = mem::take(&mut slf.callbacks);
                        let py = slf.py();
                        for (callback, context) in callbacks {
                            slf.aio_loop.call_method(
                                py,
                                "call_soon",
                                (callback, &slf),
                                Some(vec![("context", context)].into_py_dict(py)),
                            )?;
                        }
                        FutureState::Ready(result)
                    }
                };
            }
            FutureState::Executing => unimplemented!(),
            FutureState::Ready(_) => panic!("Double callback"),
        };
        leave_task(slf.py(), &slf.aio_loop, &slf)?;
        Ok(())
    }

    fn get_loop(&self) -> &PyObject {
        &self.aio_loop
    }

    fn add_done_callback(&mut self, callback: PyObject, context: Option<PyObject>) {
        self.callbacks.push((callback, context));
    }

    fn cancelled(&self) -> bool {
        false
    }

    fn result(&mut self) -> PyResult<Option<PyObject>> {
        match mem::take(&mut self.future) {
            FutureState::Ready(Err(error)) => Err(error),
            FutureState::Ready(Ok(result)) => {
                self.future = FutureState::Ready(Ok(result.clone()));
                Ok(Some(result))
            }
            FutureState::Executing => unimplemented!(),
            pending => {
                self.future = pending;
                Ok(None)
            }
        }
    }

    fn exception(&mut self) -> Option<PyErr> {
        match mem::take(&mut self.future) {
            FutureState::Ready(Err(error)) => Some(error),
            state => {
                self.future = state;
                None
            }
        }
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
        let aio_loop = get_running_loop(py)?;
        let task_like = Py::new(
            py,
            RustTask {
                future: Default::default(),
                aio_loop: aio_loop.clone(),
                callbacks: vec![],
                _asyncio_future_blocking: true,
            },
        )?;
        let wrapper = task_like.clone();
        task_like.try_borrow_mut(py)?.future = FutureState::Pending {
            future: self.0.into(),
            waker: Arc::new(AsyncioWaker {
                aio_loop: aio_loop.clone(),
                wrapper,
            }),
        };
        aio_loop.call_method1(py, "call_soon", (&task_like,))?;
        register_task(py, &task_like)?;
        task_like.convert(py)
    }
}
