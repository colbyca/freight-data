FROM python:3.12-alpine

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies
RUN apk add --no-cache \
    postgresql-client \
    nodejs \
    npm \
    curl \
    bash

RUN apk add --no-cache build-base && \
    apk add --no-cache python3-dev py3-numpy openblas-dev lapack-dev

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Node.js dependencies
COPY package.json package-lock.json ./
RUN npm install

# Copy application code
COPY . .

EXPOSE 5000

CMD ["npm", "run", "dev"]