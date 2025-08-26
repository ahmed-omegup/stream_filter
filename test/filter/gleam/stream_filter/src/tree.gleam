import bravo/uset
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor

const split_threshold = 10

const min_process_leaves = 32

pub fn get(set: Store, id: NodeId) -> Node {
  let assert Ok(node) = uset.lookup(set, id)
  node
}

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

pub type MsgToParent {
  NewBatchId(batch_id: Int)
}

pub type Node {
  Node(
    min: Int,
    max: Int,
    mid: Int,
    full: List(Customer),
    value: Value,
    notify_parent: fn(MsgToParent) -> Nil,
    pid: Option(Subject(NodeMessage)),
    all_leaf_count: Int,
    current_batch: Int,
  )
}

pub type Dir {
  Left
  Right
}

pub type NodeMessage {
  GetState(Subject(Node))
  Insert(Customer, fn(MsgToParent) -> Nil)
  Dispatch(date: Int, fun: fn(Int) -> Nil)
  SetBatch(batch_id: Int)
  BatchComplete(batch_id: Int, node_id: NodeId)
  Stop
}

fn redistribute(
  cs: List(Customer),
  set: Store,
  l_id: NodeId,
  l: Node,
  r_id: NodeId,
  r: Node,
  mid: Int,
  leaf_count: Int,
) {
  case cs {
    [] -> leaf_count
    [c, ..rest] -> {
      let leaf_count = route_customer(c, set, l_id, l, r_id, r, mid)
      redistribute(rest, set, l_id, l, r_id, r, mid, leaf_count)
    }
  }
}

fn count_leaves(n: Node) {
  case n.pid {
    None -> n.all_leaf_count
    _ -> 1
  }
}

fn route_customer(
  c: Customer,
  set: Store,
  l_id: NodeId,
  l: Node,
  r_id: NodeId,
  r: Node,
  mid: Int,
) {
  let Customer(_, start, stop) = c
  let lc = case start <= mid {
    True -> insert(set, l_id, l, c)
    False -> count_leaves(l)
  }
  let rc = case stop > mid {
    True -> insert(set, r_id, r, c)
    False -> count_leaves(r)
  }
  lc + rc
}

pub fn insert(set: Store, node_id: NodeId, node: Node, c: Customer) {
  send_to_node(
    set,
    node_id,
    node,
    Insert(c, fn(msg) {
      case msg {
        NewBatchId(batch_id) ->
          send_to_node(
            set,
            node_id,
            node,
            BatchComplete(batch_id, node_id),
            void,
          )
      }
    }),
    id,
  )
}

pub fn void(_: a) -> Nil {
  Nil
}

pub fn id(x: a) -> a {
  x
}

fn send_to_node_id(
  set: Store,
  node_id: NodeId,
  msg: NodeMessage,
  f: fn(Int) -> a,
) -> a {
  send_to_node(set, node_id, get(set, node_id), msg, f)
}

fn send_to_node(
  set: Store,
  node_id: NodeId,
  node: Node,
  msg: NodeMessage,
  f: fn(Int) -> a,
) -> a {
  let c = case node.pid {
    Some(pid) -> {
      actor.send(pid, msg)
      1
    }
    None -> {
      handle1(Actor(node_id, set), msg).0
    }
  }
  f(c)
}

pub fn new_node(
  actor: Actor,
  min: Int,
  max: Int,
  notifier: fn(MsgToParent) -> Nil,
) {
  update(
    actor,
    Node(
      min: min,
      max: max,
      mid: { min + max } / 2,
      full: [],
      value: Partial([]),
      notify_parent: notifier,
      pid: None,
      all_leaf_count: 1,
      current_batch: 0,
    ),
  )
}

pub fn handle(actor: Actor, msg: NodeMessage) -> actor.Next(Actor, NodeMessage) {
  let #(_, continue) = handle1(actor, msg)
  case continue {
    True -> actor.continue(actor)
    False -> actor.stop()
  }
}

pub fn handle1(actor: Actor, msg: NodeMessage) -> #(Int, Bool) {
  let Actor(id, set) = actor
  case msg {
    GetState(reply) -> {
      let node = get(set, id)
      actor.send(reply, node)
      #(count_leaves(node), True)
    }
    Insert(customer, notify_self) -> {
      #(insert_customer(actor, customer, notify_self), True)
    }
    Dispatch(date, fun) -> {
      #(dispatch(actor, date, fun), True)
    }
    SetBatch(batch_id) -> {
      #(set_batch(actor, batch_id), True)
    }
    BatchComplete(batch_id, node_id) -> {
      #(handle_batch_complete(actor, batch_id, node_id), True)
    }
    Stop -> #(0, False)
  }
}

pub fn start(actor: Actor, node: Node, f: fn(Subject(NodeMessage)) -> a) -> a {
  let assert Ok(actor.Started(_, pid)) =
    actor.new(actor)
    |> actor.on_message(handle)
    |> actor.start
  update(actor, Node(..node, pid: Some(pid)))
  f(pid)
}

fn update(actor: Actor, node: Node) {
  let assert Ok(Nil) = uset.insert(actor.set, actor.id, node)
  node
}

fn insert_customer(
  actor: Actor,
  customer: Customer,
  notify_self: fn(MsgToParent) -> Nil,
) -> Int {
  let node = get(actor.set, actor.id)
  let Node(min, max, mid, ..) = node
  let Customer(_, s, e) = customer
  case s <= min && e >= max {
    True -> {
      update(actor, Node(..node, full: [customer, ..node.full]))
      count_leaves(node)
    }
    False -> {
      let node = case node.value {
        Partial(data) ->
          case list.length(data) + 1 > split_threshold && min != max {
            True -> {
              let l_id = WithBatchId(actor.id * 2, 0)
              let r_id = WithBatchId(actor.id * 2 + 1, 0)
              let left =
                new_node(Actor(l_id.node, actor.set), min, mid, notify_self)
              let right =
                new_node(Actor(r_id.node, actor.set), mid, max, notify_self)
              let all_leaf_count =
                redistribute(
                  [customer, ..data],
                  actor.set,
                  l_id.node,
                  left,
                  r_id.node,
                  right,
                  mid,
                  node.all_leaf_count,
                )
              update(
                actor,
                Node(
                  ..node,
                  full: [],
                  value: Parent(l_id, r_id),
                  all_leaf_count:,
                ),
              )
            }
            False -> {
              update(actor, Node(..node, value: Partial([customer, ..data])))
            }
          }
        Parent(WithBatchId(l_id, _), WithBatchId(r_id, _)) -> {
          let left = get(actor.set, l_id)
          let right = get(actor.set, r_id)
          let all_leaf_count =
            route_customer(customer, actor.set, l_id, left, r_id, right, mid)
          update(actor, Node(..node, all_leaf_count:))
        }
      }
      let Nil = start_or_stop(actor, node)
      count_leaves(node)
    }
  }
}

fn start_or_stop(actor: Actor, node: Node) -> Nil {
  case node.all_leaf_count >= 2 * min_process_leaves {
    True -> {
      case node.pid {
        None -> start(actor, node, void)
        _ -> Nil
      }
    }
    _ ->
      case node.all_leaf_count < min_process_leaves {
        True ->
          case node.pid {
            Some(pid) -> {
              update(actor, Node(..node, pid: None))
              actor.send(pid, Stop)
            }
            None -> Nil
          }
        _ -> Nil
      }
  }
}

pub fn dispatch(actor: Actor, date: Int, fun: fn(Int) -> Nil) -> Int {
  let Actor(id, set) = actor
  let node = get(set, id)
  let Node(mid:, full:, value:, ..) = node
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
        True -> send_to_node_id(set, left.node, Dispatch(date, fun), void)
        False -> send_to_node_id(set, right.node, Dispatch(date, fun), void)
      }
    }
  }
  list.each(full, fn(item) { fun(item.id) })
  count_leaves(node)
}

fn set_batch(actor: Actor, batch_id: Int) -> Int {
  let node = get(actor.set, actor.id)
  count_leaves(case node.value {
    Parent(left, right) -> {
      // Forward batch ID to children
      send_to_node_id(actor.set, left.node, SetBatch(batch_id), void)
      send_to_node_id(actor.set, right.node, SetBatch(batch_id), void)
      update(actor, Node(..node, current_batch: batch_id))
    }
    Partial(_) -> {
      // Leaf node - Instantly notify parent
      node.notify_parent(NewBatchId(batch_id))
      update(actor, Node(..node, current_batch: batch_id))
    }
  })
}

fn handle_batch_complete(
  actor: Actor,
  completed_batch_id: Int,
  child_node_id: NodeId,
) -> Int {
  let node = get(actor.set, actor.id)
  count_leaves(case node.value {
    Parent(
      WithBatchId(left_node, left_batch),
      WithBatchId(right_node, right_batch),
    ) -> {
      // Determine which child completed based on node_id
      let is_left = child_node_id == left_node

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
      let new_node = Node(..node, value: new_value)

      // Notify parent when both children have completed the same batch
      case
        new_left.batch >= completed_batch_id
        && new_right.batch >= completed_batch_id
      {
        True -> node.notify_parent(NewBatchId(completed_batch_id))
        False -> Nil
      }
      update(actor, new_node)
    }
    Partial(_) -> {
      // Leaf nodes shouldn't receive BatchComplete messages
      node
    }
  })
}
