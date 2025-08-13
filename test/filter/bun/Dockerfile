FROM oven/bun:1 as base
WORKDIR /app

# Copy package files
COPY package.json bun.lock* ./

# Install dependencies
RUN bun install --frozen-lockfile

# Copy source code
COPY . .

# Expose ports
EXPOSE 8080

# Run the application
CMD ["bun", "run", "index.ts"]
