import { createServer, createConnection, Socket } from 'node:net';
import { TreeNode, type Event } from './interval-tree.ts';
import { customers, MAX_DATE } from './customers.ts';

// Environment variables
const PRODUCER_PORT = parseInt(process.env.PRODUCER_PORT || "8080");
const ROUTER_HOST = process.env.ROUTER_HOST || "router";
const ROUTER_PORT = parseInt(process.env.ROUTER_PORT || "8000");

// Build interval trees per customer type
const rootsPerType: Record<string, TreeNode> = {};

for (const c of customers) {
  const root = rootsPerType[c.type] ??= new TreeNode(0, MAX_DATE);
  root.insert(c);
}

console.log(`Initialized ${Object.keys(rootsPerType).length} customer types with ${customers.length} total customers`);

// Connection to router
const router = createConnection({ host: ROUTER_HOST, port: ROUTER_PORT });

router.on('connect', () => {
  console.log(`Connected to router at ${ROUTER_HOST}:${ROUTER_PORT}`);
});

router.on('error', (err: Error) => {
  console.error('Router connection error:', err);
});

function send(customerId: number): void {
  const buf = Buffer.allocUnsafe(4);
  buf.writeUInt32BE(customerId >>> 0, 0); // 32-bit big-endian
  router.write(buf);
}

function makeFramer(onMessage: (body: Buffer) => void) {
  let buf = Buffer.alloc(0);
  return (chunk: Buffer) => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 4) {
      const len = buf.readUInt32BE(0);
      if (buf.length < 4 + len) break;
      const body = buf.subarray(4, 4 + len);
      buf = buf.subarray(4 + len);
      onMessage(body);
    }
  };
}

// TCP server for producers
const server = createServer((sock: Socket) => {
  console.log('Producer connected');
  let startedAt: number | null = null; // first packet time
  let handled = 0;

  const onData = makeFramer((body: Buffer) => {
    if (startedAt === null) startedAt = Date.now();
    handled += 1;

    let event: Event;
    try {
      event = JSON.parse(body.toString('utf8'));
    } catch (e) {
      console.error('bad JSON:', e instanceof Error ? e.message : 'unknown error');
      return;
    }

    if (event && event.done) {
      console.log('Finished processing events at', new Date().toISOString());
      return;
    }

    if (event == null || event.type == null) {
      console.warn("event missing 'type'");
      return;
    }

    const root = rootsPerType[event.type];
    if (!root) {
      console.warn(`No customers for type ${event.type}`);
      return;
    }

    // Dispatch to matching customers
    let matchCount = 0;
    root.dispatch(event, (customerId: number) => {
      matchCount++;
      send(customerId);
    });
    
    if (matchCount === 0) {
      console.warn(`No matches for event date=${event.date} type=${event.type}`);
    } else {
      console.log(`Sent ${matchCount} customer IDs to router`);
    }
  });

  sock.on('data', onData);

  function finish(tag?: string) {
    if (startedAt != null) {
      const ms = Date.now() - startedAt;
      console.log(`Duration (ms): ${ms}`);
      console.log(`Handled messages: ${handled}`);
    }
    if (tag && tag !== 'close') console.error(`producer ${tag}`);
  }

  sock.on('close', () => {
    console.log('Producer disconnected');
    finish('close');
  });
  
  sock.on('error', (e: Error) => finish(`error: ${e.message}`));
});

server.listen(PRODUCER_PORT, '0.0.0.0', () => {
  console.log(`Filter service listening on 0.0.0.0:${PRODUCER_PORT}`);
});
