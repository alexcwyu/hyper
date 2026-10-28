# Aeron gateway patches

This branch (`feature/aeron-gateway-patches`) is [hyperium/hyper](https://github.com/hyperium/hyper) **v1.12.0**
(`7f67699c`) plus one commit that changes the HTTP/1 server path for the
[webgateway-rust](https://github.com/alexcwyu/webgateway-rust) order gateway. Nothing outside `src/` changes and the
public API is unchanged. Every change is gated on the `server` feature (or on `cfg(test)`), so the client and HTTP/2
code are untouched.

The gateway is a single-threaded reactor that answers `POST /order` at about a million requests a second per core.
At that rate a wall-clock read, a channel allocation or a withheld reply is visible, which is what these patches remove.

## What changes

| # | Change | Files |
|---|--------|-------|
| 1 | Do not refresh the cached `Date` when no response will carry it | `common/date.rs`, `proto/h1/dispatch.rs`, `proto/h1/conn.rs` |
| 2 | Hand a request body that is already buffered to the service whole | `body/incoming.rs`, `proto/h1/dispatch.rs`, `proto/h1/conn.rs`, `proto/h1/decode.rs`, `proto/h1/io.rs` |
| 3 | Flush the reply that precedes an upgrade | `proto/h1/dispatch.rs`, `proto/h1/conn.rs`, `proto/h1/io.rs` |
| 4 | Bound pipeline reply coalescing | `proto/h1/io.rs`, `proto/h1/conn.rs` |

### 1. Skip the `Date` refresh when the header is off

`Dispatcher::poll_catch` calls `T::update_date()` on every poll of a connection. That samples `SystemTime::now()`
(about 22 ns) to keep a cached `Date` string current, even when `Builder::auto_date_header(false)` means no response
will write it. The patch adds `Conn::date_header_enabled()` and calls `update_date()` only when it is true.
`common/date.rs` gets a `cfg(test)` counter of wall-clock samples so a test can prove none are taken.

### 2. Whole body at the first poll

Stock Hyper gives every request body a channel (`Incoming::new_channel`): the service polls an empty body, and the
connection must be polled again to move bytes from its read buffer into the channel. For a `Content-Length` body that
the head read already pulled into the buffer, that is a channel allocation and extra passes for nothing.

`Conn::take_buffered_body` takes the body whole when all of the following hold, and otherwise changes nothing:

- the request is on a server connection that reads first (`T::should_read_first()`);
- the request did not send `Expect: 100-continue` (the body must not be read before the client is told to send it);
- the body is `Content-Length`, non-empty, and every byte is already in the read buffer (`Decoder::is_length`,
  `Buffered::take_buffered`).

The body is then `Incoming::ready(bytes)`, a new `Kind::Ready` that yields one data frame and ends. The connection is
left exactly as a normal read of that body would leave it (`Reading::KeepAlive`, keep-alive re-armed). Chunked bodies,
partial bodies, `Expect` requests and all client responses stream as before.

### 3. Flush before an upgrade

If a client sends bytes straight behind an upgrade request (a WebSocket client that speaks first), pipeline
coalescing could hold the `101 Switching Protocols` reply back for a "next request" that is really the new protocol.
`Dispatcher` now calls `Conn::poll_flush_for_upgrade` (`Buffered::poll_flush_now`, which turns coalescing off) before
it returns `Dispatched::Upgrade`.

### 4. Bounded pipeline coalescing

Stock pipeline mode (`pipeline_flush(true)`) withholds a finished reply while any input is waiting. The patch keeps
batching but bounds it, so a reply cannot be held indefinitely:

- `can_buffer()` no longer returns true just because pipeline mode is on, so the configured output capacity still
  applies;
- `poll_flush` coalesces only while the next request is complete (`!read_blocked`) and output is below capacity;
- a stalled service that leaves a complete request in the buffer is given at most 16 consecutive deferrals with
  unchanged output (self-waking each time), after which the reply is flushed; any progress or flush resets the count;
- `poll_shutdown` and a response that ends keep-alive turn coalescing off, so completed replies are not discarded;
- `set_flush_pipeline(false)` may be called with output pending (that is how it is flushed), so its debug assertion now
  applies only when enabling.

## Tests

Added in this commit:

- `proto::h1::dispatch::tests::a_server_without_the_date_header_never_samples_the_wall_clock`
- `proto::h1::io::tests::gateway_pipeline_respects_configured_output_capacity`
- `proto::h1::io::tests::gateway_pipeline_flushes_at_output_capacity_with_more_input`
- `proto::h1::io::tests::gateway_pipeline_flushes_when_next_header_is_read_blocked`
- `proto::h1::io::tests::gateway_pipeline_shutdown_flushes_with_unconsumed_next_input`

Patch 2 has its own integration test, `tests/aeron_gateway_body.rs` (`cargo test --features full --test
aeron_gateway_body`): a buffered body arrives in the first poll; pipelined bodies arrive whole and in order; a body
still on its way streams and reassembles; a request with a body does not disturb the one behind it; chunked bodies
still decode; an `Expect: 100-continue` request is answered before its body is read; and a buffered body costs the
connection no allocation of its own. The whole suite (`cargo test --features full`) passes on this branch.

## Measured effect

In webgateway-rust, one gateway, 120 connections, 128 requests in flight each, 8 s windows, two interleaved A/B runs
of four rounds on an Apple-silicon laptop, patched against stock `hyper = 1.12.0`:

| Path | Stock vs patched (median of paired rounds, all 8) |
|------|----------------------------------------------------|
| `POST /order` over REST, JSON | about **-14.5 %** (every round negative, -7 % to -28 %) |
| Order over WebSocket (JSON) | within noise |

The patches were measured together, not one at a time, so how the 14.5 % splits between 1, 2 and 4 is not known.
WebSocket requests have no body and no per-request `Date`, which is consistent with 1 and 2 being the main REST gain.

## Using it

```toml
[patch.crates-io]
hyper = { git = "https://github.com/alexcwyu/hyper", branch = "feature/aeron-gateway-patches" }
```

Pin a `rev` for a reproducible build. The branch is not rebased onto later Hyper releases; to move to one, cherry-pick
the commit onto the new tag and re-run the tests above.

## Upstream

Not proposed upstream. Patch 1 is small and general (Hyper already has `auto_date_header(false)` but still pays for the
refresh) and could be sent on its own; patch 2 changes the shape of `Incoming` and would need a design discussion.
