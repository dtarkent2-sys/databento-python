FROM python:3.12-slim

# Install build dependencies for native packages (databento-dbn requires Rust)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Install Python dependencies first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install the local databento package
COPY pyproject.toml .
COPY databento/ databento/
RUN pip install --no-cache-dir .

# Copy application code
COPY main.py .

CMD ["python", "main.py"]
