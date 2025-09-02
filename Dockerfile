# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container at /app
COPY . .

# Configure database path to use Space's persistent storage
ENV DB_FILE=/data/house_prices.db

# Make port 7860 available to the world outside this container
EXPOSE 7860

# Define the command to run your app
# Use 0.0.0.0 to allow connections from outside the container
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "7860"]
