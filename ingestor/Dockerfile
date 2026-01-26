FROM python:3.11-slim

WORKDIR /app

# Install dependencies
# aiohttp is added to support the asynchronous polling architecture
RUN pip install kafka-python aiohttp python-dotenv

# Copy the script and the CSV into the image
COPY main.py .
COPY locations.csv .

# Run the script
CMD ["python", "main.py"]