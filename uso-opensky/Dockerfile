FROM python:3.10-slim-trixie
WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["python", "-u", "productor_opensky.py"]
