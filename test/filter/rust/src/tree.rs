use std::collections::LinkedList;

const SPLIT_THRESHOLD: usize = 10;

#[derive(Debug, Clone, Copy)]
pub struct Customer {
    pub id: i32,
    pub start: i32,
    pub end: i32,
}

pub enum Value {
    Partial(LinkedList<Customer>),
    Parent(Box<TreeNode>, Box<TreeNode>),
}

pub struct TreeNode {
    pub min: i32,
    pub max: i32,
    pub mid: i32,
    pub full_customers: LinkedList<Customer>,
    pub value: Value,
}

impl TreeNode {
    pub fn new(min: i32, max: i32) -> Self {
        TreeNode {
            min,
            max,
            mid: (min + max) / 2,
            full_customers: LinkedList::new(),
            value: Value::Partial(LinkedList::new()),
        }
    }

    pub fn leaves(&self) -> i32 {
        match &self.value {
            Value::Parent(left, right) => left.leaves() + right.leaves(),
            Value::Partial(_) => 1
        }
    }
    pub fn insert(&mut self, customer: Customer) {
        let start = customer.start;
        let end = customer.end;

        // Full containment
        if start <= self.min && end >= self.max {
            self.full_customers.push_back(customer);
            return;
        }

        // Partial overlap
        match &mut self.value {
            Value::Parent(ref mut left, ref mut right) => {
                if start <= self.mid {
                    left.insert(customer);
                }
                if end > self.mid {
                    right.insert(customer);
                }
                return;
            }
            Value::Partial(ref mut list) => {
                list.push_back(customer);

                if list.len() > SPLIT_THRESHOLD && self.min != self.max {
                    // Split
                    let old_list = std::mem::replace(list, LinkedList::new());
                    self.value = Value::Parent(
                        Box::new(TreeNode::new(self.min, self.mid)),
                        Box::new(TreeNode::new(self.mid + 1, self.max)),
                    );

                    for customer in old_list {
                        self.insert_into_children(customer);
                    }
                }
            }
        }
    }

    fn insert_into_children(&mut self, customer: Customer) {
        if let Value::Parent(ref mut left, ref mut right) = self.value {
            if customer.start <= self.mid {
                left.insert(customer);
            }
            if customer.end > self.mid {
                right.insert(customer);
            }
        }
    }

    pub fn dispatch<F>(&self, date: i32, callback: &mut F)
    where
        F: FnMut(i32),
    {
        // Fast path: send to full_customers
        for customer in &self.full_customers {
            callback(customer.id);
        }

        // Handle based on value type
        match &self.value {
            Value::Partial(list) => {
                // Check partial_customers
                for customer in list {
                    if customer.start <= date && date <= customer.end {
                        callback(customer.id);
                    }
                }
            }
            Value::Parent(left, right) => {
                // Traverse children
                if date <= self.mid {
                    left.dispatch(date, callback);
                }
                if date > self.mid {
                    right.dispatch(date, callback);
                }
            }
        }
    }
}
