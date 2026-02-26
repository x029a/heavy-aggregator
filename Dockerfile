# Stage 1: Build the website
FROM node:20-alpine AS frontend-builder
WORKDIR /app/website
COPY website/package*.json ./
RUN npm ci
COPY website/ ./
RUN npm run build

# Stage 2: Python backend
FROM python:3.9-slim
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

# Copy built website from Stage 1
COPY --from=frontend-builder /app/website/dist /app/website/dist

# Create output directory
RUN mkdir -p output

# Expose port
EXPOSE 8000

# Environment
ENV PYTHONUNBUFFERED=1

# Run the API server (serves both API + static website)
ENTRYPOINT ["python", "server.py"]
