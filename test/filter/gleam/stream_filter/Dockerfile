FROM erlang:28-alpine

# Install Gleam
RUN wget https://github.com/gleam-lang/gleam/releases/download/v1.6.2/gleam-v1.6.2-x86_64-unknown-linux-musl.tar.gz && \
    tar -xzf gleam-v1.6.2-x86_64-unknown-linux-musl.tar.gz && \
    mv gleam /usr/local/bin/ && \
    rm gleam-v1.6.2-x86_64-unknown-linux-musl.tar.gz

WORKDIR /app

# Copy project files
COPY gleam.toml .
COPY src/ src/

# Download dependencies and build
RUN gleam deps download
RUN gleam build

# Export the application
RUN gleam export erlang-shipment

# Run the application
CMD ["./build/erlang-shipment/entrypoint.sh", "run"]
