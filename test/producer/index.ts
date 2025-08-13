import { connect } from "node:net";

const MAX_DATE = parseInt(process.env.MAX_DATE || "100000");
const ROUTER_HOST = process.env.ROUTER_HOST || "localhost";
const ROUTER_PORT = parseInt(process.env.ROUTER_PORT || "8080");
const EVENT_COUNT = parseInt(process.env.EVENT_COUNT || "2000000");
const MSS = parseInt(process.env.MSS || "65000"); // Maximum segment size

interface Event {
  id: number;
  date: number;
  type: string;
}

/** Generator function to create stream events */
function* handleMessages(): Generator<string, void, unknown> {
  let id = 0;
  const startTime = new Date();
  console.log("Producer started sending events at", startTime.toISOString());
  
  for (let i = 0; i < EVENT_COUNT; i++) {
    const event: Event = {
      id: id++,
      date: Math.floor(Math.random() * MAX_DATE) + 1,
      type: "type" + Math.floor(Math.random() * 10), // Random type 0-9
    };
    yield JSON.stringify(event);
  }

  console.log("Producer finished sending events at", new Date().toISOString());
  console.log("Producer took", new Date().getTime() - startTime.getTime(), "ms to send", EVENT_COUNT, "events");
}

const handleConnection = (): void => {
  console.log(`Connecting to filter at ${ROUTER_HOST}:${ROUTER_PORT}`);
  
  const socket = connect(ROUTER_PORT, ROUTER_HOST);
  
  socket.on('connect', () => {
    console.log('Connected to filter');
    let buffer = Buffer.allocUnsafe(MSS);
    let offset = 0;
    
    for (const message of handleMessages()) {
      const messageBytes = Buffer.from(message, 'utf8');
      const newOffset = offset + messageBytes.length + 4; // 4 bytes for length prefix
      
      if (newOffset > MSS) {
        // Send current buffer and start a new one
        socket.write(buffer.subarray(0, offset));
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
      socket.write(buffer.subarray(0, offset));
    }
    
    socket.end();
    console.log('Data sent, connection closed');
  });

  socket.on('error', (err: Error) => {
    console.error('Connection error:', err);
    process.exit(1);
  });

  socket.on('close', () => {
    console.log('Connection closed');
    process.exit(0);
  });
};

// Start the producer
handleConnection();
