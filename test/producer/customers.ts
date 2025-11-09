import { connect, Socket } from "node:net";
import seedrandom from "seedrandom";
const rng = seedrandom("42"); // seed = "42"

const MAX_DATE = parseInt(process.env.MAX_DATE || "100000");
const MAX_SPAN = parseInt(process.env.MAX_DATE || "10");
const ROUTER_HOST = process.env.ROUTER_HOST || "localhost";
const ROUTER_PORT = parseInt(process.env.ROUTER_PORT || "8080");
const EVENT_COUNT = parseInt(process.env.EVENT_COUNT || "2000000");
const MSS = parseInt(process.env.MSS || "65000"); // Maximum segment size



function generateCustomer(
  id: number,
  rng: () => number
): { customer: Customer, rng: () => number } {
  const start = Math.floor(rng() * (MAX_DATE - MAX_SPAN)) + 1;
  const span = Math.floor(rng() * MAX_SPAN) + 1;
  return {
    customer: { id, start, end: start + span },
    rng
  };
}

interface Customer {
  id: number;
  start: number;
  end: number;
}

interface Event {
  id: number;
  date: number;
  type: string;
}

/** Wait for filter to be ready */
async function waitForFilter(): Promise<Socket> {
  console.log(`Waiting for filter at ${ROUTER_HOST}:${ROUTER_PORT} to be ready...`);

  return new Promise<Socket>((resolve) => {
    const checkConnection = () => {
      const socket = connect(ROUTER_PORT, ROUTER_HOST);

      socket.on('connect', () => {
        console.log('Filter is ready!');
        resolve(socket);
      });

      socket.on('error', () => {
        // Filter not ready yet, try again in 1 second
        setTimeout(checkConnection, 1000);
      });
    };

    checkConnection();
  });
}

/** Generator function to create stream events */
function* handleMessages(): Generator<string, void, unknown> {
  let id = 0;
  const startTime = new Date();
  console.log("Producer started sending events at", startTime.toISOString());

  for (let i = 0; i < EVENT_COUNT; i++) {
    const event: Event = {
      id: id++,
      date: Math.floor(rng() * MAX_DATE) + 1,
      type: "type" + Math.floor(rng() * 10), // Random type 0-9
    };
    yield JSON.stringify(event);
  }

  console.log("Producer finished sending events at", new Date().toISOString());
  console.log("Producer took", new Date().getTime() - startTime.getTime(), "ms to send", EVENT_COUNT, "events");
}

const handleConnection = async (socket: Socket): Promise<void> => {
  console.log(`Connected to filter at ${ROUTER_HOST}:${ROUTER_PORT}`);
  socket.on('error', (err: Error) => {
    console.error('Connection error:', err);
    process.exit(1);
  });

  socket.on('close', () => {
    console.log('Connection closed');
    process.exit(0);
  });

  // Helper function to write with backpressure
  const writeWithBackpressure = async (data: Buffer): Promise<void> => {
    return new Promise((resolve) => {
      const canWriteMore = socket.write(data);

      if (canWriteMore) {
        // Write was successful, can continue immediately
        resolve();
      } else {
        setTimeout(resolve, 50)
      }
    })
  };

  console.log('Connected to filter');
  let buffer = Buffer.allocUnsafe(MSS);
  let offset = 0;
  let buffersSent = 0;

  for (const message of handleMessages()) {
    const messageBytes = Buffer.from(message, 'utf8');
    const newOffset = offset + messageBytes.length + 4; // 4 bytes for length prefix

    if (newOffset > MSS) {
      // Send current buffer and start a new one
      await writeWithBackpressure(buffer.subarray(0, offset));
      buffersSent++;
      buffer = Buffer.allocUnsafe(MSS);
      offset = 0;
    }

    // Write length prefix (4 bytes) followed by message
    buffer.writeUInt32BE(messageBytes.length, offset);
    messageBytes.copy(buffer, offset + 4);
    offset += messageBytes.length + 4;
  }

  // Send remaining data
  if (offset > 0) {
    await writeWithBackpressure(buffer.subarray(0, offset));
    buffersSent++;
  }

  socket.end();
};

// Start the producer after waiting for filter to be ready
async function main() {
  await handleConnection(await waitForFilter());
}

main().catch((err) => {
  console.error('Producer error:', err);
  process.exit(1);
});
