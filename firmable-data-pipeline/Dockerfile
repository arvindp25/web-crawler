# Dockerfile

FROM python:3.12-slim

WORKDIR /app

# Install pip deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create volume mount point
VOLUME ["/app/abr_xml_data"]
# RUN python -m spacy download en_core_web_sm
ENV DBT_PROFILES_DIR=/app/dbt
CMD ["python", "run.py"]
