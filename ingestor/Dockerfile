FROM python:3.11-slim

WORKDIR /app

# Install dependencies
# aiohttp to support the asynchronous polling.
# todo: better pyenv management. Poetry?
RUN pip install kafka-python aiohttp python-dotenv

# Copy the script and the CSV into the image
COPY main.py .
COPY locations.csv .

# Run the script
CMD ["python", "main.py"]
