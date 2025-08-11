# Use the official Bun image
FROM oven/bun:1 as base

# Set the working directory
WORKDIR /app

# Copy package files
COPY package.json bun.lock* ./

# Install dependencies
RUN bun install --frozen-lockfile

# Copy source code
COPY . .

# Expose the port the app runs on
EXPOSE 8000

# Run the application
CMD ["bun", "run", "start"]
