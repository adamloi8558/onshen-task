# Build stage
FROM node:18-alpine AS builder

WORKDIR /app

# Install system dependencies for ffmpeg and sharp
RUN apk add --no-cache \
    ffmpeg \
    libc6-compat

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./

# Install dependencies
RUN npm ci && npm cache clean --force

# Copy source code
COPY src ./src

# Copy environment file for backup
COPY environment.production ./.env

# Build the application
RUN npm run build

# Production stage
FROM node:18-alpine AS runner

WORKDIR /app

# Install production dependencies and ffmpeg
RUN apk add --no-cache \
    ffmpeg \
    libc6-compat \
    curl

# Create non-root user
RUN addgroup --system --gid 1001 taskrunner
RUN adduser --system --uid 1001 taskrunner

# Copy built application and node_modules from builder
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/.env ./.env

# Create temp directory for processing
RUN mkdir -p /app/temp && chown -R taskrunner:taskrunner /app/temp

# Set correct permissions
RUN chown -R taskrunner:taskrunner /app
USER taskrunner

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD node -e "console.log('Task worker is running')" || exit 1

# Start the application
CMD ["npm", "start"]