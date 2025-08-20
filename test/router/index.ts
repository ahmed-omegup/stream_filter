import { createServer } from "node:net";

const PORT = 8000;
let start: number | null = null;

// Create a worker that does nothing on messages
const worker = new Worker(new URL("data:text/javascript,self.onmessage = () => {}", import.meta.url));

const server = createServer((socket) => {
  console.log(`Client connected`);

  socket.on("data", (chunk: Buffer) => {
    if (start === null) {
      start = Date.now();
    }

    worker.postMessage(chunk); // send to no-op worker
  });

  socket.on("end", () => {
    console.log("Client disconnected");
        const duration = start ? Date.now() - start : NaN;
        console.log(`Duration (ms): ${duration}`);
        socket.destroy();
        server.close();
        worker.terminate();
        return;
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Listening on 0.0.0.0:${PORT}`);
});
