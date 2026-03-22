# Use a light version of Python
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies for database connection
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Copy and install Python libraries
COPY requirements .
RUN pip install --no-cache-dir -r requirements
RUN pip install --upgrade pip

# Copy your Python scripts
COPY app/ .

# Run the tracker!
CMD ["python", "main.py"]