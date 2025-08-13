import type { Customer } from './customers.ts';

export interface Event {
  id: number;
  date: number;
  type: string;
  done?: boolean;
}

export type SendCallback = (customerId: number) => void;

export class TreeNode {
  min: number;
  max: number;
  mid: number;
  fullCustomers: Customer[] = [];
  partialCustomers: Customer[] = [];
  left: TreeNode | null = null;
  right: TreeNode | null = null;

  constructor(min: number, max: number) {
    this.min = min;
    this.max = max;
    this.mid = Math.floor((min + max) / 2);
  }

  insert(customer: Customer): void {
    const { start, end } = customer;

    // Full containment
    if (start <= this.min && end >= this.max) {
      this.fullCustomers.push(customer);
      return;
    }

    // Partial overlap
    this.partialCustomers.push(customer);

    if (this.partialCustomers.length > 10 && this.min !== this.max) {
      // Split
      if (!this.left && !this.right) {
        this.left = new TreeNode(this.min, this.mid);
        this.right = new TreeNode(this.mid + 1, this.max);
      }

      const reinsert = [...this.partialCustomers];
      this.partialCustomers = [];

      for (const c of reinsert) {
        this._insertIntoChildren(c);
      }
    }
  }

  private _insertIntoChildren(customer: Customer): void {
    if (customer.end <= this.mid) {
      this.left!.insert(customer);
    } else if (customer.start > this.mid) {
      this.right!.insert(customer);
    } else {
      // Customer spans both sides
      if (customer.splitsLeft) {
        // Duplicate customer into both children with decremented splits
        const leftCustomer: Customer = { ...customer, splitsLeft: customer.splitsLeft - 1 };
        const rightCustomer: Customer = { ...customer, splitsLeft: customer.splitsLeft - 1 };
        
        this.left!.insert(leftCustomer);
        this.right!.insert(rightCustomer);
      } else {
        // No splits left, keep in partial
        this.partialCustomers.push(customer);
      }
    }
  }

  dispatch(event: Event, send: SendCallback): void {
    const date = event.date;

    // Fast path: send to fullCustomers
    for (const c of this.fullCustomers) {
      send(c.id);
    }

    // Check partialCustomers
    for (const c of this.partialCustomers) {
      if (c.start <= date && date <= c.end) {
        send(c.id);
      }
    }

    // Traverse children
    if (this.left && date <= this.mid) {
      this.left.dispatch(event, send);
    } else if (this.right && date > this.mid) {
      this.right.dispatch(event, send);
    }
  }
}
