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

# Run the data pipeline script to ensure the database exists
RUN python3 data_pipeline.py

# Make port 7860 available to the world outside this container
EXPOSE 7860

# Define the command to run your app
# Use 0.0.0.0 to allow connections from outside the container
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "7860"]

