import bravo/uset
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/set

const split_threshold = 10

const process_threshold = 64

pub type Customer {
  Customer(id: Int, start: Int, stop: Int)
}

pub type Store =
  uset.USet(NodeId, Node)

pub type Actor {
  Actor(id: NodeId, set: Store)
}

type NodeId =
  Int

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
  BatchComplete(batch_id: Int, node_id: NodeId)
}

fn redistribute(cs: List(Customer), set: Store, l: NodeId, r: NodeId, mid: Int) {
  case cs {
    [] -> Nil
    [c, ..rest] -> {
      route_customer(c, set, l, r, mid)
      redistribute(rest, set, l, r, mid)
    }
  }
}

fn route_customer(c: Customer, set: Store, l: NodeId, r: NodeId, mid: Int) {
  let Customer(_, start, stop) = c
  case start <= mid {
    True ->
      send_to_node_id(
        set,
        l,
        Insert(c, fn(batch_id) {
          send_to_node_id(set, l, BatchComplete(batch_id, l))
        }),
      )
    False -> Nil
  }
  case stop > mid {
    True ->
      send_to_node_id(
        set,
        r,
        Insert(c, fn(batch_id) {
          send_to_node_id(set, r, BatchComplete(batch_id, r))
        }),
      )
    False -> Nil
  }
}

// TODO: This needs to be implemented to look up node by ID and 
// either send message to process or handle recursively
fn send_to_node_id(set: Store, node_id: NodeId, msg: NodeMessage) {
  let assert Ok(node) = uset.lookup(set, node_id)
  case node.pid {
    Some(pid) -> actor.send(pid, msg)
    None -> {
      handle1(Actor(node_id, set), msg)
      Nil
    }
  }
}

pub fn new_node(min: Int, max: Int, notifier: fn(Int) -> Nil) {
  Node(
    min: min,
    max: max,
    mid: { min + max } / 2,
    full: [],
    value: Partial([]),
    notify_parent: notifier,
    pid: None,
    current_batch: 0,
  )
}

pub fn handle(state: Actor, msg: NodeMessage) -> actor.Next(Actor, NodeMessage) {
  let Nil = handle1(state, msg)
  actor.continue(state)
}

pub fn handle1(actor: Actor, msg: NodeMessage) -> Nil {
  let Actor(id, set) = actor
  case msg {
    GetState(reply) -> {
      let assert Ok(state) = uset.lookup(set, id)
      actor.send(reply, state)
    }
    Insert(customer, notify_self) -> {
      insert_customer(actor, customer, notify_self)
    }
    Dispatch(date, fun) -> {
      dispatch(actor, date, fun)
    }
    SetBatch(batch_id) -> {
      set_batch(actor, batch_id)
    }
    BatchComplete(batch_id, path) -> {
      handle_batch_complete(actor, batch_id, path)
    }
  }
}

fn update(actor: Actor, node: Node) {
  let assert Ok(Nil) = uset.insert(actor.set, actor.id, node)
  Nil
}

fn insert_customer(
  actor: Actor,
  customer: Customer,
  notify_self: fn(Int) -> Nil,
) -> Nil {
  let assert Ok(state) = uset.lookup(actor.set, actor.id)
  let Node(min, max, mid, ..) = state
  let Customer(_, s, e) = customer
  case s <= min && e >= max {
    True -> update(actor, Node(..state, full: [customer, ..state.full]))
    False ->
      case state.value {
        Partial(data) ->
          case list.length(data) + 1 > split_threshold && min != max {
            True -> {
              let left = WithBatchId(actor.id * 2, 0)
              let right = WithBatchId(actor.id * 2 + 1, 0)
              let Nil =
                redistribute(
                  [customer, ..data],
                  actor.set,
                  left.node,
                  right.node,
                  mid,
                )
              update(actor, Node(..state, full: [], value: Parent(left, right)))
            }
            False ->
              update(actor, Node(..state, value: Partial([customer, ..data])))
          }
        Parent(WithBatchId(left, _), WithBatchId(right, _)) -> {
          route_customer(customer, actor.set, left, right, mid)
        }
      }
  }
}

fn dispatch(actor: Actor, date: Int, fun: fn(Int) -> Nil) {
  let assert Ok(state) = uset.lookup(actor.set, actor.id)
  let Node(mid:, full:, value:, ..) = state
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
        True ->
          send_to_node_id(actor.set, left.node, Dispatch(date, fun))
        False ->
          send_to_node_id(actor.set, right.node, Dispatch(date, fun))
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
  let Node(min, max, mid, full, value, notify_parent, pid, leaf_count, _) =
    state
  case value {
    Parent(left, right) -> {
      // Forward batch ID to children
      send_to_node_id(left.node, SetBatch(batch_id))
      send_to_node_id(right.node, SetBatch(batch_id))
      Node(
        min: min,
        max: max,
        mid: mid,
        full: full,
        value: value,
        notify_parent: notify_parent,
        pid: pid,
        leaf_count: leaf_count,
        current_batch: batch_id,
      )
    }
    Partial(_) -> {
      // Leaf node - Instantly notify parent
      notify_parent(batch_id)
      Node(
        min: min,
        max: max,
        mid: mid,
        full: full,
        value: value,
        notify_parent: notify_parent,
        pid: pid,
        leaf_count: leaf_count,
        current_batch: batch_id,
      )
    }
  }
}

fn handle_batch_complete(
  state: Node,
  completed_batch_id: Int,
  path: List(Dir),
) -> Node {
  let Node(
    min,
    max,
    mid,
    full,
    value,
    notify_parent,
    pid,
    leaf_count,
    current_batch,
  ) = state
  case value {
    Parent(
      WithBatchId(left_node, left_batch),
      WithBatchId(right_node, right_batch),
    ) -> {
      // Determine which child completed based on path
      let is_left = case path {
        [Left, ..] -> True
        _ -> False
      }

      // Update the batch ID for the child that completed
      let new_left = case is_left {
        True -> WithBatchId(left_node, completed_batch_id)
        False -> WithBatchId(left_node, left_batch)
      }
      let new_right = case is_left {
        True -> WithBatchId(right_node, right_batch)
        False -> WithBatchId(right_node, completed_batch_id)
      }

      let new_value = Parent(new_left, new_right)
      let new_state =
        Node(
          min: min,
          max: max,
          mid: mid,
          full: full,
          value: new_value,
          notify_parent: notify_parent,
          pid: pid,
          leaf_count: leaf_count,
          current_batch: current_batch,
        )

      // Notify parent when both children have completed the same batch
      case
        new_left.batch >= completed_batch_id
        && new_right.batch >= completed_batch_id
      {
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
