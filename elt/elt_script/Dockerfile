FROM python:3.8-slim


RUN apt-get update && apt-get install -y postgresql-client-15


COPY elt_pipeline.py .


CMD ["python", "elt_pipeline.py"]