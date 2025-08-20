import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor

const split_threshold = 10

pub type Customer {
  Customer(id: Int, start: Int, stop: Int)
}

pub type Value {
  Partial(List(Customer))
  Parent(Subject(NodeMessage), Subject(NodeMessage))
}

pub type Node {
  Node(min: Int, max: Int, mid: Int, full: List(Customer), value: Value)
}

pub type NodeMessage {
  GetState(Subject(Node))
  Insert(Customer)
  Dispatch(date: Int, fun: fn(Int) -> Nil)
}

fn redistribute(
  cs: List(Customer),
  l: Subject(NodeMessage),
  r: Subject(NodeMessage),
  mid: Int,
) {
  case cs {
    [] -> Nil
    [c, ..rest] -> {
      route_customer(c, l, r, mid)
      redistribute(rest, l, r, mid)
    }
  }
}

fn route_customer(
  c: Customer,
  l: Subject(NodeMessage),
  r: Subject(NodeMessage),
  mid: Int,
) {
  let Customer(_, start, stop) = c
  case start <= mid {
    True -> actor.send(l, Insert(c))
    False -> Nil
  }
  case stop > mid {
    True -> actor.send(r, Insert(c))
    False -> Nil
  }
}

pub fn start(
  min: Int,
  max: Int,
) -> Result(actor.Started(Subject(NodeMessage)), actor.StartError) {
  let node = Node(min, max, { min + max } / 2, [], Partial([]))

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
    Insert(customer) -> {
      let new_state = insert_customer(state, customer)
      actor.continue(new_state)
    }
    Dispatch(date, fun) -> {
      dispatch(state, date, fun)
      actor.continue(state)
    }
  }
}

fn insert_customer(state: Node, customer: Customer) -> Node {
  let Node(min, max, mid, full, value) = state
  let Customer(_, s, e) = customer
  case s <= min && e >= max {
    True -> Node(min, max, mid, [customer, ..full], value)
    False ->
      case value {
        Partial(data) ->
          case list.length(data) + 1 > split_threshold && min != max {
            True -> {
              let assert Ok(actor.Started(_, left)) = start(min, mid)
              let assert Ok(actor.Started(_, right)) = start(mid + 1, max)
              redistribute([customer, ..data], left, right, mid)
              Node(min, max, mid, full, Parent(left, right))
            }
            False -> Node(min, max, mid, full, Partial([customer, ..data]))
          }
        Parent(left, right) -> {
          route_customer(customer, left, right, mid)
          state
        }
      }
  }
}

fn dispatch(state: Node, date: Int, fun: fn(Int) -> Nil) {
  let Node(_, _, mid, full, value) = state
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
        True -> actor.send(left, Dispatch(date, fun))
        False -> actor.send(right, Dispatch(date, fun))
      }
    }
  }
  list.each(full, fn(item) {
    case item {
      Customer(id, _, _) -> fun(id)
    }
  })
}
