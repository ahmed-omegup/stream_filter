import gleam/bit_array
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import glisten/socket
import glisten/socket/options.{ActiveMode, Passive}
import glisten/tcp
import mug
import prng/random
import prng/seed
import tree
import envoy

const max_date = 100_000

const max_span = 10

const num_customers = 100_000

type BufferState {
  BufferState(size: Int, buffer: BitArray)
  PendingBytes(bytes: BitArray)
  Nothing
}

type GlobalState {
  GlobalState(
    root: process.Subject(tree.NodeMessage),
    event_decoder: decode.Decoder(Event),
    router: mug.Socket,
  )
}

pub fn main() {
  let event_decoder = {
    use id <- decode.field("id", decode.int)
    use date <- decode.field("date", decode.int)
    use typ <- decode.field("type", decode.string)
    decode.success(Event(id:, date:, typ:))
  }
  
  // Get router host and port from environment variables
  let router_host = case envoy.get("ROUTER_HOST") {
    Ok(host) -> host
    Error(_) -> "localhost"
  }
  let router_port = case envoy.get("ROUTER_PORT") {
    Ok(port_str) -> case int.parse(port_str) {
      Ok(port) -> port
      Error(_) -> 8000
    }
    Error(_) -> 8000
  }
  
  let listen_res = tcp.listen(8080, [ActiveMode(Passive)])
  case listen_res {
    Ok(listener) -> {
      let assert Ok(root) = tree.start(1, max_date)
      let customers = generate_customers(num_customers)
      customers
      |> list.each(fn(c) { actor.send(root.data, tree.Insert(c)) })
      let assert Ok(router_socket) =
        mug.new(router_host, port: router_port)
        |> mug.timeout(milliseconds: 5000)
        |> mug.connect()

      let assert Ok(actor.Started(_, stream)) =
        actor.new(GlobalState(
          root: root.data,
          event_decoder: event_decoder,
          router: router_socket,
        ))
        |> actor.on_message(handle_message)
        |> actor.start

      io.println("Listening on 0.0.0.0:8080")
      case tcp.accept(listener) {
        Ok(socket) -> {
          case recv(socket, None, 0, Nothing, stream) {
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
    Error(e) -> {
      io.println("Listen failed")
      io.println_error(string.inspect(e))
    }
  }
}

fn recv(
  socket: socket.Socket,
  start_opt: Option(timestamp.Timestamp),
  messages: Int,
  buffer: BufferState,
  stream: process.Subject(BitArray),
) -> Result(#(Int, Int), socket.SocketReason) {
  case tcp.receive(socket, 0) {
    Ok(chunk) -> {
      case start_opt {
        None -> {
          recv_timed(
            socket,
            messages,
            chunk,
            buffer,
            timestamp.system_time(),
            stream,
          )
        }
        Some(start) -> {
          recv_timed(socket, messages, chunk, buffer, start, stream)
        }
      }
    }
    Error(e) -> {
      case start_opt {
        None -> {
          Error(e)
        }
        Some(start) -> {
          let now = timestamp.system_time()
          let d = timestamp.difference(start, now)
          let #(secs, nanos) = duration.to_seconds_and_nanoseconds(d)
          let ms_from_ns = nanos / 1_000_000
          Ok(#(secs * 1000 + ms_from_ns, messages))
        }
      }
    }
  }
}

fn recv_timed(
  socket: socket.Socket,
  messages: Int,
  chunk: BitArray,
  buffer: BufferState,
  start: timestamp.Timestamp,
  stream: process.Subject(BitArray),
) -> Result(#(Int, Int), socket.SocketReason) {
  let #(buffer, messages) = handle_prefixed(buffer, chunk, messages, stream)
  recv(socket, Some(start), messages, buffer, stream)
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
        state.root,
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

fn generate_customer(id: Int) -> tree.Customer {
  let start = random.sample(random.int(1, max_date - max_span), seed.random())
  let span = random.sample(random.int(1, max_span), seed.random())
  tree.Customer(id, start, start + span)
}

fn generate_customers(n: Int) -> List(tree.Customer) {
  list.range(1, n)
  |> list.map(generate_customer)
}
