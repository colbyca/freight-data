FROM python:3.12-alpine

# Install Poetry
RUN apk add --no-cache curl && \
    curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy pyproject.toml and poetry.lock
COPY pyproject.toml poetry.lock ./
COPY ./package.json .
COPY ./package-lock.json .

# Install dependencies using Poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Install Node.js and npm
RUN apk add --no-cache nodejs npm
RUN npm install

COPY . .

EXPOSE 5000

CMD ["npm", "run", "dev"]