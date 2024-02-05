FROM python:3.9


WORKDIR /utube-parser


COPY utube_parser.py .
COPY __init__.py .
COPY data_loader.py .
COPY schemas.py .
COPY config.json .
COPY ./google_credentials ./google_credentials
COPY requirements.txt .



RUN pip install -r requirements.txt


CMD ["python", "./utube_parser.py"]