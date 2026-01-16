# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create output directory
RUN mkdir -p output

# Define environment variable
ENV PYTHONUNBUFFERED=1

# Run main.py when the container launches
ENTRYPOINT ["python", "main.py"]
# Default arguments (can be overridden)
CMD ["--help"]
