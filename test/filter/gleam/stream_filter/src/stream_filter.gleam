import bravo
import bravo/uset
import envoy
import gleam/bit_array
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import glisten/socket
import glisten/socket/options.{ActiveMode, Any, Buffer, Ip, Passive}
import glisten/tcp
import mug
import prng/random
import prng/seed
import tree.{id}

type BufferState {
  BufferState(size: Int, buffer: BitArray)
  PendingBytes(bytes: BitArray)
  Nothing
}

type BatchMessage {
  BatchCompleted(Int)
}

type Batch {
  Batch(current: Int, waiting: Int, packets: Int)
}

type GlobalState {
  GlobalState(
    root_actor: process.Subject(tree.NodeMessage),
    event_decoder: decode.Decoder(Event),
    router: mug.Socket,
  )
}

fn env(env: String, default: a, parse: fn(String) -> Result(a, Nil)) -> a {
  result.unwrap(result.try(envoy.get(env), parse), default)
}

pub fn main() {
  let event_decoder = {
    use id <- decode.field("id", decode.int)
    use date <- decode.field("date", decode.int)
    use typ <- decode.field("type", decode.string)
    decode.success(Event(id:, date:, typ:))
  }

  // Get router host and port from environment variables
  let router_host = env("ROUTER_HOST", "127.0.0.1", Ok)
  let router_port = env("ROUTER_PORT", 8000, int.parse)
  let num_customers = env("NUM_CUSTOMERS", 100_000, int.parse)
  let max_date = env("MAX_DATE", 100_000, int.parse)
  let max_span = env("MAX_SPAN", 10, int.parse)

  let listen_res =
    tcp.listen(8080, [
      ActiveMode(Passive),
      Ip(Any),
      Buffer(10_240),
      // Very small buffer - should fill quickly
      options.Nodelay(False),
      // Enable Nagle algorithm to batch packets
      options.SendTimeout(100),
      // Short send timeout
      options.Backlog(10),
      // Very small connection backlog
    ])
  let assert Ok(listener) = listen_res as "Listen failed"

  // Create batch completion subject
  let batch_subject = process.new_subject()

  // Create root node
  let root_id = 1

  let assert Ok(store) = uset.new("state", bravo.Public)

  let root = tree.Actor(root_id, store)

  let node =
    tree.new_node(root, 1, max_date, fn(msg) {
      case msg {
        tree.NewBatchId(batch_id) ->
          actor.send(batch_subject, BatchCompleted(batch_id))
      }
    })

  // Create root actor
  let root_actor = tree.start(root, node, id)

  let customers = generate_customers(num_customers, max_date, max_span)
  customers
  |> list.each(fn(c) { tree.insert(store, root_id, node, c) })

  let assert Ok(router) =
    mug.new(router_host, port: router_port)
    |> mug.timeout(milliseconds: 5000)
    |> mug.connect()
    as "Couldn't Connect to router"

  let assert Ok(actor.Started(_, stream)) =
    actor.new(GlobalState(root_actor:, event_decoder:, router:))
    |> actor.on_message(handle_message)
    |> actor.start

  io.println("Listening on 0.0.0.0:8080")
  case tcp.accept(listener) {
    Ok(socket) -> {
      case
        recv(socket, None, 0, Nothing, stream, batch_subject, Batch(1, 0, 0))
      {
        Ok(#(ms, n)) -> {
          io.println("Duration (ms): " <> int.to_string(ms))
          io.println("Handled messages: " <> int.to_string(n))
        }
        Error(e) -> {
          io.print_error("Receive loop failed: ")
          io.println_error(string.inspect(e))
        }
      }
    }
    Error(e) -> {
      io.println("Accept failed")
      io.println_error(string.inspect(e))
    }
  }
}

fn time_from(start: timestamp.Timestamp) {
  let now = timestamp.system_time()
  let d = timestamp.difference(start, now)
  let #(secs, nanos) = duration.to_seconds_and_nanoseconds(d)
  let ms_from_ns = nanos / 1_000_000
  secs * 1000 + ms_from_ns
}

fn recv(
  socket: socket.Socket,
  start_opt: Option(timestamp.Timestamp),
  messages: Int,
  buffer: BufferState,
  stream: process.Subject(BitArray),
  batch_subject: process.Subject(BatchMessage),
  batch: Batch,
) -> Result(#(Int, Int), socket.SocketReason) {
  case batch.waiting > 3 {
    True -> {
      case process.receive(batch_subject, 1000) {
        Ok(BatchCompleted(_)) ->
          proceed_recv(
            socket,
            start_opt,
            messages,
            buffer,
            stream,
            batch_subject,
            Batch(..batch, waiting: batch.waiting - 1),
          )
        Error(e) -> {
          io.println(
            "Failed to receive batch completion: " <> string.inspect(e),
          )
          recv(
            socket,
            start_opt,
            messages,
            buffer,
            stream,
            batch_subject,
            batch,
          )
        }
      }
    }
    False ->
      proceed_recv(
        socket,
        start_opt,
        messages,
        buffer,
        stream,
        batch_subject,
        batch,
      )
  }
}

fn proceed_recv(
  socket: socket.Socket,
  start_opt: Option(timestamp.Timestamp),
  messages: Int,
  buffer: BufferState,
  stream: process.Subject(BitArray),
  batch_subject: process.Subject(BatchMessage),
  batch: Batch,
) -> Result(#(Int, Int), socket.SocketReason) {
  case tcp.receive(socket, 0) {
    Ok(chunk) -> {
      let start = case start_opt {
        None -> timestamp.system_time()
        Some(start) -> start
      }
      let #(buffer, messages) = handle_prefixed(buffer, chunk, messages, stream)
      let Batch(current:, waiting:, packets:) = batch
      let new_batch = case packets == 1000 {
        True -> {
          actor.send(batch_subject, BatchCompleted(current))
          Batch(current: current + 1, waiting: waiting + 1, packets: 0)
        }
        False -> {
          Batch(..batch, packets: packets + 1)
        }
      }
      recv(
        socket,
        Some(start),
        messages,
        buffer,
        stream,
        batch_subject,
        new_batch,
      )
    }
    Error(e) -> {
      case start_opt {
        None -> {
          Error(e)
        }
        Some(start) -> {
          Ok(#(time_from(start), messages))
        }
      }
    }
  }
}

fn split(chunk: BitArray, size: Int) -> Option(#(BitArray, BitArray)) {
  case bit_array.slice(chunk, 0, size) {
    Ok(start) -> {
      let remaining = bit_array.byte_size(chunk) - size
      case bit_array.slice(chunk, size, remaining) {
        Ok(end) -> Some(#(start, end))
        Error(_) -> None
      }
    }
    Error(_) -> None
  }
}

type Event {
  Event(id: Int, date: Int, typ: String)
}

fn handle_message(state: GlobalState, msg: BitArray) {
  case json.parse_bits(msg, state.event_decoder) {
    Ok(event) -> {
      actor.send(
        state.root_actor,
        tree.Dispatch(event.date, fn(customer_id) {
          let assert Ok(_) = mug.send(state.router, <<customer_id:32>>)
          Nil
        }),
      )
    }
    Error(e) -> {
      io.println_error("Failed to decode JSON message: " <> string.inspect(e))
    }
  }
  actor.continue(state)
}

fn handle_chunk(
  chunk: BitArray,
  messages: Int,
  stream: process.Subject(BitArray),
) -> #(BufferState, Int) {
  case chunk {
    <<>> -> #(Nothing, messages)
    <<len:32, rest:bits>> -> {
      case split(rest, len) {
        Some(#(start, end)) -> {
          actor.send(stream, start)
          handle_chunk(end, messages + 1, stream)
        }
        None -> #(BufferState(len, rest), messages)
      }
    }
    bytes -> #(PendingBytes(bytes), messages)
  }
}

fn handle_prefixed(
  state: BufferState,
  chunk: BitArray,
  messages: Int,
  stream: process.Subject(BitArray),
) -> #(BufferState, Int) {
  case state {
    PendingBytes(bytes) ->
      handle_chunk(bit_array.append(bytes, chunk), messages, stream)
    Nothing -> handle_chunk(chunk, messages, stream)
    BufferState(size, prefix) -> {
      case split(chunk, size - bit_array.byte_size(prefix)) {
        Some(#(start, end)) -> {
          actor.send(stream, bit_array.append(prefix, start))
          handle_chunk(end, messages + 1, stream)
        }
        None -> #(BufferState(size, bit_array.append(prefix, chunk)), messages)
      }
    }
  }
}

fn generate_customer(
  id: Int,
  max_date: Int,
  max_span: Int,
  seed: seed.Seed,
) -> #(tree.Customer, seed.Seed) {
  let #(start, seed) = random.step(random.int(1, max_date - max_span), seed)
  let #(span, seed) = random.step(random.int(1, max_span), seed)
  #(tree.Customer(id, start, start + span), seed)
}

fn generate_customers(
  n: Int,
  max_date: Int,
  max_span: Int,
) -> List(tree.Customer) {
  let seed = seed.new(42)
  let list: List(tree.Customer) = []
  let #(list, _) =
    list.range(1, n)
    |> list.fold(#(list, seed), fn(acc, date) {
      let #(list, seed) = acc
      let #(customer, seed) = generate_customer(date, max_date, max_span, seed)
      #([customer, ..list], seed)
    })
  list
}
