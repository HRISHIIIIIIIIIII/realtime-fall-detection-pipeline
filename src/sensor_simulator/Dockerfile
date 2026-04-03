# Base image — slim Python (small footprint, ~45MB)
FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Copy and install dependencies first (Docker caches this layer)
# If requirements.txt doesn't change, Docker won't re-install packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY simulator.py .

# Run the simulator
# -u flag: unbuffered output — ensures print() appears immediately
# in docker compose logs (without it, Python buffers stdout)
CMD ["python", "-u", "simulator.py"]
