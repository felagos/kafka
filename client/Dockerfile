FROM oven/bun:1.0.25 as builder

WORKDIR /app

# Copy package.json and related files
COPY package.json bun.lock* bunfig.toml* ./

# Install dependencies
RUN bun install

# Copy source code
COPY . .

# Build the application
RUN bun build ./client.ts --outdir ./dist --target bun

# Create a smaller production image
FROM oven/bun:1.0.25-slim

WORKDIR /app

# Copy only the necessary files from the builder stage
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./

# Set environment variables
ENV NODE_ENV=production

# Start the application
CMD ["bun", "run", "dist/client.js"]
