use {
    futures::{
        future::{BoxFuture, FutureExt},
        stream::{BoxStream, Stream, StreamExt},
        task::{waker_ref, ArcWake},
    },
    once_cell::sync::OnceCell,
    pyo3::{
        callback::IntoPyCallbackOutput,
        exceptions::{asyncio::CancelledError, PyStopAsyncIteration},
        ffi,
        iter::IterNextOutput,
        prelude::*,
        pyasync::IterANextOutput,
        type_object::PyTypeObject,
        types::IntoPyDict,
        PyAsyncProtocol, PyIterProtocol, PyTypeInfo,
    },
    std::{
        future::Future,
        marker::PhantomData,
        mem,
        sync::Arc,
        task::{Context, Poll},
    },
};

fn monkey_patch_into_accepted_coro_types<T: PyTypeInfo>(py: Python) -> PyResult<&()> {
    static MONKEY_PATCHED: OnceCell<()> = OnceCell::new();
    MONKEY_PATCHED.get_or_try_init(|| {
        let coroutines = PyModule::import(py, "asyncio.coroutines")?;
        let typecache_set: &pyo3::types::PySet =
            coroutines.getattr("_iscoroutine_typecache")?.extract()?;
        typecache_set.add(T::type_object(py))?;
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

fn cancelled_error() -> PyErr {
    CancelledError::new_err("Rustine cancelled")
}

enum FutureState {
    Cancelled,
    Pending {
        future: BoxFuture<'static, PyResult<PyObject>>,
        waker: Arc<AsyncioWaker>,
    },
    Executing,
}

/// Rust Coroutine
#[pyclass(weakref, module = "pyo3_futures")]
struct Rustine {
    future: FutureState,
    aio_loop: PyObject,
    callbacks: Vec<(PyObject, Option<PyObject>)>,
    #[pyo3(get, set)]
    _asyncio_future_blocking: bool,
}

#[pyproto]
impl PyAsyncProtocol for Rustine {
    fn __await__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
}

#[pyproto]
impl PyIterProtocol for Rustine {
    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<IterNextOutput<PyRefMut<Self>, PyObject>> {
        let mut execution_slot = FutureState::Executing;
        mem::swap(&mut execution_slot, &mut slf.future);
        let result = match &mut execution_slot {
            FutureState::Pending { future, waker } => slf.py().allow_threads(|| {
                let waker_ref = waker_ref(&waker);
                let context = &mut Context::from_waker(&*waker_ref);
                future.as_mut().poll(context)
            }),
            FutureState::Cancelled => Poll::Ready(Err(cancelled_error())),
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
    rustine: Py<Rustine>,
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
        Rustine::schedule_callbacks(slf.rustine.try_borrow_mut(slf.py())?)
    }
}

#[pymethods]
impl Rustine {
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

    /// https://docs.python.org/3/library/asyncio-future.html?highlight=asyncio%20future#asyncio.Future.result
    fn result(&self) -> PyResult<Option<PyObject>> {
        match self.future {
            FutureState::Cancelled => Err(cancelled_error()),
            FutureState::Pending { .. } => Ok(None),
            _ => unimplemented!(),
        }
    }

    /// https://docs.python.org/3/library/asyncio-future.html?highlight=asyncio%20future#asyncio.Future.cancel
    fn cancel(mut slf: PyRefMut<Self>) -> PyResult<()> {
        slf.future = FutureState::Cancelled;
        Rustine::schedule_callbacks(slf)
    }

    /// https://docs.python.org/3/library/asyncio-future.html?highlight=asyncio%20future#asyncio.Future.cancelled
    fn cancelled(&self) -> bool {
        match self.future {
            FutureState::Cancelled => true,
            _ => false,
        }
    }
}

impl Rustine {
    /// https://github.com/python/cpython/blob/17ef4319a34f5a2f95e7823dfb5f5b8cff11882d/Lib/asyncio/futures.py#L159
    fn schedule_callbacks(mut slf: PyRefMut<Self>) -> PyResult<()> {
        if slf.callbacks.is_empty() {
            panic!("nothing to call back")
        }
        let callbacks = std::mem::take(&mut slf.callbacks);
        let py = slf.py();
        for (callback, context) in callbacks {
            slf.aio_loop.call_method(
                py,
                "call_soon",
                (callback, &slf),
                Some(vec![("context", context)].into_py_dict(py)),
            )?;
        }
        Ok(())
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
        monkey_patch_into_accepted_coro_types::<Rustine>(py)?;
        let aio_loop = get_running_loop(py)?;
        let rustine = Py::new(
            py,
            Rustine {
                future: FutureState::Cancelled,
                aio_loop: aio_loop.clone(),
                callbacks: vec![],
                _asyncio_future_blocking: false,
            },
        )?;
        let clone = rustine.clone();
        rustine.try_borrow_mut(py)?.future = FutureState::Pending {
            future: self.0.into(),
            waker: Arc::new(AsyncioWaker {
                aio_loop,
                rustine: clone,
            }),
        };
        rustine.convert(py)
    }
}

enum StreamState {
    Cancelled,
    Pending {
        stream: BoxStream<'static, PyResult<PyObject>>,
        waker: Arc<PowerDamWaker>,
    },
    Executing,
}
/// AsyncGenerator from a Stream
/// https://docs.python.org/3/glossary.html#term-asynchronous-iterator
#[pyclass(weakref, module = "pyo3_futures")]
struct PowerDam {
    stream: StreamState,
    aio_loop: PyObject,
    callbacks: Vec<(PyObject, Option<PyObject>)>,
    #[pyo3(get, set)]
    _asyncio_future_blocking: bool,
}

#[pyproto]
impl PyAsyncProtocol for PowerDam {
    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__(slf: PyRef<Self>) -> IterANextOutput<PyRef<Self>, ()> {
        IterANextOutput::Yield(slf)
    }

    fn __await__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
}

#[pyproto]
impl PyIterProtocol for PowerDam {
    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<IterNextOutput<PyRefMut<Self>, PyObject>> {
        let mut execution_slot = StreamState::Executing;
        mem::swap(&mut execution_slot, &mut slf.stream);
        let result = match &mut execution_slot {
            StreamState::Pending { stream, waker } => slf.py().allow_threads(|| {
                let waker_ref = waker_ref(&waker);
                let context = &mut Context::from_waker(&*waker_ref);
                stream.as_mut().poll_next(context)
            }),
            StreamState::Cancelled => Poll::Ready(Some(Err(cancelled_error()))),
            _ => unimplemented!(),
        };
        mem::swap(&mut execution_slot, &mut slf.stream);
        match result {
            Poll::Pending => {
                slf._asyncio_future_blocking = true;
                Ok(IterNextOutput::Yield(slf))
            }
            Poll::Ready(Some(result)) => Ok(IterNextOutput::Return(result?)),
            Poll::Ready(None) => Err(PyStopAsyncIteration::new_err(())),
        }
    }
}

#[pymethods]
impl PowerDam {
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

    /// https://docs.python.org/3/library/asyncio-future.html?highlight=asyncio%20future#asyncio.Future.result
    fn result(&self) -> PyResult<Option<PyObject>> {
        match self.stream {
            StreamState::Cancelled => Err(cancelled_error()),
            StreamState::Pending { .. } => Ok(None),
            _ => unimplemented!(),
        }
    }

    /// https://docs.python.org/3/library/asyncio-future.html?highlight=asyncio%20future#asyncio.Future.cancel
    fn cancel(mut slf: PyRefMut<Self>) -> PyResult<()> {
        slf.stream = StreamState::Cancelled;
        PowerDam::schedule_callbacks(slf)
    }

    /// https://docs.python.org/3/library/asyncio-future.html?highlight=asyncio%20future#asyncio.Future.cancelled
    fn cancelled(&self) -> bool {
        match self.stream {
            StreamState::Cancelled => true,
            _ => false,
        }
    }
}

impl PowerDam {
    /// https://github.com/python/cpython/blob/17ef4319a34f5a2f95e7823dfb5f5b8cff11882d/Lib/asyncio/futures.py#L159
    fn schedule_callbacks(mut slf: PyRefMut<Self>) -> PyResult<()> {
        if slf.callbacks.is_empty() {
            panic!("nothing to call back")
        }
        let callbacks = std::mem::take(&mut slf.callbacks);
        let py = slf.py();
        for (callback, context) in callbacks {
            slf.aio_loop.call_method(
                py,
                "call_soon",
                (callback, &slf),
                Some(vec![("context", context)].into_py_dict(py)),
            )?;
        }
        Ok(())
    }
}

#[pyclass]
#[derive(Clone)]
struct PowerDamWaker {
    aio_loop: PyObject,
    powerdam: Py<PowerDam>,
}

impl ArcWake for PowerDamWaker {
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
impl PowerDamWaker {
    #[call]
    fn __call__(slf: PyRef<Self>) -> PyResult<()> {
        PowerDam::schedule_callbacks(slf.powerdam.try_borrow_mut(slf.py())?)
    }
}

struct PySendableStream(BoxStream<'static, PyResult<PyObject>>);

impl<TStream, TItem> From<TStream> for PySendableStream
where
    TStream: Stream<Item = TItem> + Send + 'static,
    TItem: IntoPyCallbackOutput<PyObject>,
{
    fn from(stream: TStream) -> Self {
        Self(
            stream
                .map(|item| Python::with_gil(move |py| item.convert(py)))
                .boxed(),
        )
    }
}

impl From<PySendableStream> for BoxStream<'static, PyResult<PyObject>> {
    fn from(wrapper: PySendableStream) -> Self {
        wrapper.0
    }
}

pub struct PyAsyncGen<T>(PySendableStream, PhantomData<T>);

impl<TStream, TItem> From<TStream> for PyAsyncGen<TItem>
where
    TStream: Stream<Item = TItem> + Send + 'static,
    TItem: IntoPyCallbackOutput<PyObject>,
{
    fn from(stream: TStream) -> Self {
        Self(stream.into(), PhantomData)
    }
}

impl<TItem> IntoPyCallbackOutput<PyObject> for PyAsyncGen<TItem>
where
    TItem: IntoPyCallbackOutput<PyObject>,
{
    fn convert(self, py: Python) -> PyResult<PyObject> {
        monkey_patch_into_accepted_coro_types::<PowerDam>(py)?;
        let aio_loop = get_running_loop(py)?;
        let powerdam = Py::new(
            py,
            PowerDam {
                stream: StreamState::Cancelled,
                aio_loop: aio_loop.clone(),
                callbacks: vec![],
                _asyncio_future_blocking: false,
            },
        )?;
        let clone = powerdam.clone();
        powerdam.try_borrow_mut(py)?.stream = StreamState::Pending {
            stream: self.0.into(),
            waker: Arc::new(PowerDamWaker {
                aio_loop,
                powerdam: clone,
            }),
        };
        powerdam.convert(py)
    }
}
