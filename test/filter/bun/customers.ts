// Environment variables with defaults
const NUM_CUSTOMERS = parseInt(process.env.NUM_CUSTOMERS || "100000");
const MAX_DATE = parseInt(process.env.MAX_DATE || "100000");
const MAX_SPAN = parseInt(process.env.MAX_SPAN || "10");
const MAX_SPLITS = parseInt(process.env.MAX_SPLITS || "-1"); // Maximum number of splits per customer

export interface Customer {
  id: number;
  start: number;
  end: number;
  type: string;
  splitsLeft: number; // Number of splits remaining, 0 means no more splits allowed
}

// Generate customers
export const customers: Customer[] = Array.from({ length: NUM_CUSTOMERS }, (_, id) => {
  const start = Math.floor(Math.random() * (MAX_DATE - MAX_SPAN));
  const span = Math.floor(Math.random() * MAX_SPAN) + 1;
  return { 
    id, 
    start, 
    end: start + span, 
    type: `type${Math.floor(Math.random() * 10)}`,
    splitsLeft: MAX_SPLITS
  };
});

export { NUM_CUSTOMERS, MAX_DATE, MAX_SPAN, MAX_SPLITS };
