// Environment variables with defaults
const NUM_CUSTOMERS = parseInt(process.env.NUM_CUSTOMERS || "100000");
const MAX_DATE = parseInt(process.env.MAX_DATE || "100000");
const MAX_SPAN = parseInt(process.env.MAX_SPAN || "10");

export interface Customer {
  id: number;
  start: number;
  end: number;
  type: string;
}

// Generate customers
export const customers: Customer[] = Array.from({ length: NUM_CUSTOMERS }, (_, id) => {
  const start = Math.floor(Math.random() * (MAX_DATE - MAX_SPAN));
  const span = Math.floor(Math.random() * MAX_SPAN) + 1;
  return { 
    id, 
    start, 
    end: start + span, 
    type: `type${Math.floor(Math.random() * 10)}` 
  };
});

export { NUM_CUSTOMERS, MAX_DATE, MAX_SPAN };
