//! Aeron gateway patch 2 (see PATCHES.md): a `Content-Length` request body that already sits in the read buffer is
//! handed to the service whole, at the first poll, instead of streaming it through a channel that needs further
//! passes of the connection to fill. Everything else about bodies must stay as it was.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Request, Response};

/// Counts the allocations made by the calling thread, so a test sees what one connection costs and nothing else.
struct Counting;
thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
fn count_one() {
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
}
// SAFETY: every call is forwarded unchanged to the system allocator; the counter is a const thread-local `Cell`.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_one();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_one();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count_one();
        unsafe { System.realloc(pointer, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// What the peer has sent so far (`feed` adds to it) and what the server wrote back.
#[derive(Default)]
struct Wire {
    input: VecDeque<u8>,
    output: Vec<u8>,
}
struct Peer(Rc<RefCell<Wire>>);
impl Read for Peer {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        mut buffer: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let mut wire = self.0.borrow_mut();
        let count = {
            let (front, _) = wire.input.as_slices();
            if front.is_empty() {
                return Poll::Pending;
            }
            let count = front.len().min(buffer.remaining());
            buffer.put_slice(&front[..count]);
            count
        };
        wire.input.drain(..count);
        Poll::Ready(Ok(()))
    }
}
impl Write for Peer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.borrow_mut().output.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// An empty response body.
struct Nothing;
impl Body for Nothing {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(None)
    }
    fn is_end_stream(&self) -> bool {
        true
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(0)
    }
}

/// How one request's body reached the service.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    /// Whether the very first poll of the body already produced its data.
    first_poll_had_data: bool,
    body: Vec<u8>,
}
struct Reader(Rc<RefCell<Vec<Seen>>>);
impl Service<Request<Incoming>> for Reader {
    type Response = Response<Nothing>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response<Nothing>, Infallible>>>>;
    fn call(&self, request: Request<Incoming>) -> Self::Future {
        let seen = self.0.clone();
        Box::pin(async move {
            let mut body = request.into_body();
            let mut first = true;
            let mut record = Seen {
                first_poll_had_data: false,
                body: Vec::new(),
            };
            loop {
                let frame = std::future::poll_fn(|cx| {
                    let polled = Pin::new(&mut body).poll_frame(cx);
                    if first {
                        first = false;
                        record.first_poll_had_data =
                            matches!(&polled, Poll::Ready(Some(Ok(frame))) if frame.is_data());
                    }
                    polled
                })
                .await;
                match frame {
                    Some(Ok(frame)) => {
                        if let Ok(data) = frame.into_data() {
                            record.body.extend_from_slice(&data);
                        }
                    }
                    _ => break,
                }
            }
            seen.borrow_mut().push(record);
            Ok(Response::new(Nothing))
        })
    }
}

/// Reads a request's body frame by frame and keeps nothing of it, so it adds no allocation beyond its own future.
struct Sink;
impl Service<Request<Incoming>> for Sink {
    type Response = Response<Nothing>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response<Nothing>, Infallible>>>>;
    fn call(&self, request: Request<Incoming>) -> Self::Future {
        Box::pin(async move {
            let mut body = request.into_body();
            while let Some(Ok(_)) =
                std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
            {}
            Ok(Response::new(Nothing))
        })
    }
}

struct Run {
    wire: Rc<RefCell<Wire>>,
    seen: Rc<RefCell<Vec<Seen>>>,
    connection: Pin<Box<dyn Future<Output = hyper::Result<()>>>>,
}
impl Run {
    fn new() -> Self {
        let seen = Rc::new(RefCell::new(Vec::new()));
        Self::serving(Reader(seen.clone()), seen)
    }
    fn serving<S>(service: S, seen: Rc<RefCell<Vec<Seen>>>) -> Self
    where
        S: Service<Request<Incoming>, Response = Response<Nothing>, Error = Infallible> + 'static,
        S::Future: 'static,
    {
        let wire = Rc::new(RefCell::new(Wire::default()));
        let mut builder = http1::Builder::new();
        builder
            .keep_alive(true)
            .pipeline_flush(true)
            .auto_date_header(false)
            .header_read_timeout(None);
        let connection = Box::pin(builder.serve_connection(Peer(wire.clone()), service));
        Self {
            wire,
            seen,
            connection,
        }
    }
    /// The peer sends `bytes`; the connection runs until it has nothing more to do.
    fn feed(&mut self, bytes: &[u8]) {
        self.wire.borrow_mut().input.extend(bytes);
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..16 {
            let _ = self.connection.as_mut().poll(&mut cx);
        }
    }
    fn answers(&self) -> usize {
        let wire = self.wire.borrow();
        String::from_utf8_lossy(&wire.output)
            .matches("HTTP/1.1 200 OK")
            .count()
    }
}

fn post(body: &str) -> Vec<u8> {
    format!(
        "POST /order HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

#[test]
fn a_body_that_arrived_with_its_head_is_in_the_first_poll() {
    let mut run = Run::new();
    run.feed(&post("{\"clOrdId\":1}"));
    assert_eq!(
        *run.seen.borrow(),
        [Seen {
            first_poll_had_data: true,
            body: b"{\"clOrdId\":1}".to_vec()
        }]
    );
    assert_eq!(run.answers(), 1);
}

#[test]
fn pipelined_bodies_each_arrive_whole_in_order() {
    let mut run = Run::new();
    let mut wire = Vec::new();
    for n in 0..5 {
        wire.extend(post(&format!("body-{n}")));
    }
    run.feed(&wire);
    let seen = run.seen.borrow();
    assert_eq!(seen.len(), 5);
    for (n, seen) in seen.iter().enumerate() {
        assert!(seen.first_poll_had_data, "request {n}");
        assert_eq!(seen.body, format!("body-{n}").into_bytes());
    }
    drop(seen);
    assert_eq!(run.answers(), 5);
}

#[test]
fn a_body_still_on_its_way_is_streamed_and_reassembled() {
    let mut run = Run::new();
    let request = post("0123456789");
    let (head, tail) = request.split_at(request.len() - 4);
    run.feed(head);
    assert!(
        run.seen.borrow().is_empty(),
        "the service must wait for the rest of the body"
    );
    run.feed(tail);
    assert_eq!(
        *run.seen.borrow(),
        [Seen {
            // The six bytes that came with the head are in the first poll; the last four follow.
            first_poll_had_data: true,
            body: b"0123456789".to_vec()
        }]
    );
    assert_eq!(run.answers(), 1);
    // The connection is still in step: the next request is read from the right place.
    run.feed(&post("next"));
    assert_eq!(run.seen.borrow().len(), 2);
    assert_eq!(run.seen.borrow()[1].body, b"next");
}

#[test]
fn a_request_with_a_body_does_not_disturb_the_one_behind_it() {
    let mut run = Run::new();
    let mut wire = post("abc");
    wire.extend(b"GET /work?input=1 HTTP/1.1\r\nHost: x\r\n\r\n");
    wire.extend(post("defg"));
    run.feed(&wire);
    let seen = run.seen.borrow();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].body, b"abc");
    assert_eq!(seen[1].body, b"");
    assert_eq!(seen[2].body, b"defg");
    drop(seen);
    assert_eq!(run.answers(), 3);
}

#[test]
fn a_chunked_body_is_still_decoded() {
    let mut run = Run::new();
    run.feed(b"POST /order HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n");
    assert_eq!(run.seen.borrow().len(), 1);
    assert_eq!(run.seen.borrow()[0].body, b"abcde");
    assert_eq!(run.answers(), 1);
}

#[test]
fn a_continue_request_is_answered_before_its_body_is_read() {
    let mut run = Run::new();
    run.feed(
        b"POST /order HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nExpect: 100-continue\r\n\r\n",
    );
    assert!(
        String::from_utf8_lossy(&run.wire.borrow().output).contains("HTTP/1.1 100 Continue"),
        "the interim answer is written once the service asks for the body"
    );
    assert!(run.seen.borrow().is_empty());
    run.feed(b"abc");
    assert_eq!(run.seen.borrow().len(), 1);
    assert_eq!(run.seen.borrow()[0].body, b"abc");
}

/// The allocations `run` makes to take in `wire` and answer it, with its answer then thrown away.
fn allocated_serving(run: &mut Run, wire: &[u8]) -> usize {
    let before = allocations();
    run.feed(wire);
    let used = allocations() - before;
    run.wire.borrow_mut().output.clear();
    used
}

#[test]
fn a_buffered_body_costs_the_connection_no_allocation_of_its_own() {
    let get = b"GET /order HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
    let post = post("{\"clOrdId\":1}");
    let mut without = Run::serving(Sink, Default::default());
    let mut with = Run::serving(Sink, Default::default());
    // Let both connections reach their working size first.
    for _ in 0..16 {
        allocated_serving(&mut without, &get);
        allocated_serving(&mut with, &post);
    }
    let rounds = 64;
    let (mut bare, mut bodied) = (0, 0);
    for _ in 0..rounds {
        bare += allocated_serving(&mut without, &get);
        bodied += allocated_serving(&mut with, &post);
    }
    eprintln!("allocations over {rounds} requests: without a body {bare}, with a body {bodied}");
    assert!(
        bodied <= bare,
        "{rounds} requests with a body allocated {bodied} times, the same without one {bare}: the body must not need a channel"
    );
}
