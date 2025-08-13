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
  let buf: Buffer = Buffer.alloc(0);
  return (chunk: Buffer) => {
    buf = buf.length ? Buffer.concat([buf, chunk]) : chunk;
    while (buf.length >= 4) {
      const len = buf.readUInt32BE(0);
      if (buf.length < 4 + len) break;
      const body = buf.subarray(4, 4 + len);
      buf = buf.subarray(4 + len);
      onMessage(body);
    }
  };
}

// Message queue for processing
const messageQueue: Buffer[] = [];
let processing = false;
let startedAt: number | null = null;
let handled = 0;

// Process messages from queue
function processMessages() {
  if (processing || messageQueue.length === 0) return;
  processing = true;

  while (messageQueue.length > 0) {
    const body = messageQueue.shift()!;
    
    if (startedAt === null) {
      console.log('First event received, starting timer...');
      startedAt = Date.now();
    }
    handled += 1;

    let event: Event;
    try {
      event = JSON.parse(body.toString('utf8'));
    } catch (e) {
      console.error('bad JSON:', e instanceof Error ? e.message : 'unknown error');
      continue;
    }

    if (event && event.done) {
      console.log('Received done event, finishing...');
      finishProcessing();
      break;
    }

    if (event == null || event.type == null) {
      console.warn("event missing 'type'");
      continue;
    }

    const root = rootsPerType[event.type];
    if (!root) {
      console.warn(`No customers for type ${event.type}`);
      continue;
    }

    // Dispatch to matching customers
    try {
      root.dispatch(event, (customerId: number) => send(customerId));
    } catch (e) {
      console.error(`Error during dispatch for event ${handled}:`, e instanceof Error ? e.message : 'unknown error');
      console.error(`Event details: id=${event.id}, date=${event.date}, type=${event.type}`);
    }
  }

  processing = false;
  
  // Check if more messages arrived while processing
  if (messageQueue.length > 0) {
    setImmediate(processMessages);
  }
}

function finishProcessing() {
  console.log('Finished processing events at', new Date().toISOString());
  if (startedAt != null) {
    const ms = Date.now() - startedAt;
    console.log(`Duration (ms): ${ms}`);
    console.log(`Handled messages: ${handled}`);
  }
  router.end();
  server.close()
}

// TCP server for producers - focuses only on reading and queuing
const server = createServer((sock: Socket) => {
  console.log('Producer connected');

  const onData = makeFramer((body: Buffer) => {
    // Just queue the message, don't process it here
    messageQueue.push(body);
    
    // Trigger processing if not already running
    if (!processing) {
      setImmediate(processMessages);
    }
  });

  sock.on('data', onData);

  sock.on('close', () => {
    console.log('Producer disconnected');
    // Process any remaining messages
    if (messageQueue.length > 0) {
      processMessages();
    }
    finishProcessing();
  });
  
  sock.on('error', (e: Error) => {
    console.error(`Producer socket error: ${e.message}`);
  });
});

server.listen(PRODUCER_PORT, '0.0.0.0', () => {
  console.log(`Filter service listening on 0.0.0.0:${PRODUCER_PORT}`);
});
