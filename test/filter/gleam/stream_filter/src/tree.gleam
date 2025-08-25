import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/io
import gleam/int

const split_threshold = 10
const process_threshold = 64

pub type Customer {
  Customer(id: Int, start: Int, stop: Int)
}

type NodeId = Int

pub type WithBatchId {
  WithBatchId(node: NodeId, batch: Int)
}
pub type Value {
  Partial(List(Customer))
  Parent(WithBatchId, WithBatchId)
}

pub type Node {
  Node(
    min: Int,
    max: Int,
    mid: Int,
    full: List(Customer),
    value: Value,
    notify_parent: fn(Int) -> Nil,
    pid: Option(Subject(NodeMessage)),
    leaf_count: Int,
    current_batch: Int,
  )
}

pub type Dir {
  Left
  Right
}

pub type NodeMessage {
  GetState(Subject(Node))
  Insert(Customer, fn(Int) -> Nil)
  Dispatch(date: Int, fun: fn(Int) -> Nil)
  SetBatch(batch_id: Int)
  BatchComplete(batch_id: Int, path: List(Dir))
}

fn redistribute(cs: List(Customer), l: WithBatchId, r: WithBatchId, mid: Int) {
  case cs {
    [] -> Nil
    [c, ..rest] -> {
      route_customer(c, l, r, mid)
      redistribute(rest, l, r, mid)
    }
  }
}

fn route_customer(c: Customer, l: WithBatchId, r: WithBatchId, mid: Int) {
  let Customer(_, start, stop) = c
  case start <= mid {
    True ->
      actor.send(
        l.chan,
        Insert(c, fn(batch_id) { actor.send(l.chan, BatchComplete(batch_id, True)) }),
      )
    False -> Nil
  }
  case stop > mid {
    True ->
      actor.send(
        r.chan,
        Insert(c, fn(batch_id) { actor.send(r.chan, BatchComplete(batch_id, False)) }),
      )
    False -> Nil
  }
}

pub fn start(
  min: Int,
  max: Int,
  notifier: fn(Int) -> Nil,
) -> Result(actor.Started(Subject(NodeMessage)), actor.StartError) {
  let node = Node(min, max, { min + max } / 2, [], Partial([]), notifier)

  actor.new(node)
  |> actor.on_message(handle)
  |> actor.start
}

pub fn handle(state: Node, msg: NodeMessage) -> actor.Next(Node, NodeMessage) {
  case msg {
    GetState(reply) -> {
      actor.send(reply, state)
      actor.continue(state)
    }
    Insert(customer, notify_self) -> {
      let new_state = insert_customer(state, customer, notify_self)
      actor.continue(new_state)
    }
    Dispatch(date, fun) -> {
      dispatch(state, date, fun)
      actor.continue(state)
    }
    SetBatch(batch_id) -> {
      let new_state = set_batch(state, batch_id)
      actor.continue(new_state)
    }
    BatchComplete(batch_id, is_left) -> {
      let new_state = handle_batch_complete(state, batch_id, is_left)
      actor.continue(new_state)
    }
  }
}

fn insert_customer(
  state: Node,
  customer: Customer,
  notify_self: fn(Int) -> Nil,
) -> Node {
  let Node(min, max, mid, full, value, notify_parent) = state
  let Customer(_, s, e) = customer
  case s <= min && e >= max {
    True -> Node(min, max, mid, [customer, ..full], value, notify_parent)
    False ->
      case value {
        Partial(data) ->
          case list.length(data) + 1 > split_threshold && min != max {
            True -> {
              // Create children that will notify this node when batch completes
              // But we need the subject reference of this node to create the notifier
              // This is the chicken-and-egg problem we discussed earlier
              let assert Ok(actor.Started(_, left_chan)) =
                start(min, mid, notify_self)
              let assert Ok(actor.Started(_, right_chan)) =
                start(mid + 1, max, notify_self)
              let left = WithBatchId(left_chan, 0)
              let right = WithBatchId(right_chan, 0)
              redistribute([customer, ..data], left, right, mid)
              Node(min, max, mid, full, Parent(left, right), notify_parent)
            }
            False ->
              Node(
                min,
                max,
                mid,
                full,
                Partial([customer, ..data]),
                notify_parent,
              )
          }
        Parent(left, right) -> {
          route_customer(customer, left, right, mid)
          state
        }
      }
  }
}

fn dispatch(state: Node, date: Int, fun: fn(Int) -> Nil) {
  let Node(_, _, mid, full, value, _) = state
  case value {
    Partial(data) -> {
      list.each(data, fn(item) {
        case item {
          Customer(id, s, e) ->
            case date >= s && date <= e {
              True -> fun(id)
              False -> Nil
            }
        }
      })
    }
    Parent(left, right) -> {
      case date <= mid {
        True -> actor.send(left.chan, Dispatch(date, fun))
        False -> actor.send(right.chan, Dispatch(date, fun))
      }
    }
  }
  list.each(full, fn(item) {
    case item {
      Customer(id, _, _) -> fun(id)
    }
  })
}

fn set_batch(state: Node, batch_id: Int) -> Node {
  let Node(min, max, mid, full, value, notify_parent) = state
  case value {
    Parent(left, right) -> {
      // Forward batch ID to children
      actor.send(left.chan, SetBatch(batch_id))
      actor.send(right.chan, SetBatch(batch_id))
      Node(min, max, mid, full, value, notify_parent)
    }
    Partial(_) -> {
      // Leaf node - Instantly notify parent
      notify_parent(batch_id)
      state
    }
  }
}

fn handle_batch_complete(state: Node, completed_batch_id: Int, is_left: Bool) -> Node {
  let Node(min, max, mid, full, value, notify_parent) = state
  case value {
    Parent(WithBatchId(left_chan, left_batch), WithBatchId(right_chan, right_batch)) -> {
      // Update the batch ID for the child that completed
      let new_left = case is_left {
        True -> WithBatchId(left_chan, completed_batch_id)
        False -> WithBatchId(left_chan, left_batch)
      }
      let new_right = case is_left {
        True -> WithBatchId(right_chan, right_batch)
        False -> WithBatchId(right_chan, completed_batch_id)
      }
      
      let new_value = Parent(new_left, new_right)
      let new_state = Node(min, max, mid, full, new_value, notify_parent)

      // Notify parent when both children have completed the same batch
      case new_left.batch >= completed_batch_id && new_right.batch >= completed_batch_id {
        True -> notify_parent(completed_batch_id)
        False -> Nil
      }
      new_state
    }
    Partial(_) -> {
      // Leaf nodes shouldn't receive BatchComplete messages
      state
    }
  }
}
