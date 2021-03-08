# PyO3 Futures

Compatibility wrappers for using async Rust from Python.

## Implementation

This crate relies on mimicking the behaviour of [`asyncio.Future`](https://docs.python.org/3/library/asyncio-future.html#future-object) and is therefore only compatible with `asyncio`-compatible loops.
(If you don't know what that means, you probably don't have to worry about it.)

For a detailed explanation on the approach, see [here](https://github.com/ThibaultLemaire/Async-PyO3-Examples#implementing-a-python-future-in-rust).

### Existing Work

In the unlikely event that you don't know about it, there is an other crate, [`pyo3-asyncio`](https://github.com/awestlake87/pyo3-asyncio), that achieves the same result but by spawning a runtime on a different thread and using a [`futures::channel::oneshot`](https://docs.rs/futures/latest/futures/channel/oneshot/index.html) to send the result back to Python.

`pyo3-futures` (this crate) is an exploration on a different approach: Letting Python drive the future like any other coroutine (so, on the same thread).
Although `pyo3-asyncio` achieves runtime agnosticity through generics (you can choose between `async-std` or `tokio`), `pyo3-futures` should be more flexible in that it doesn't even have a concept of "runtime".

## Examples

[`sterling`](https://gitlab.com/ThibaultLemaire/sterling) is an async MongoDB client for Python that I'm developing as an example of using this crate to build async Python libraries.
