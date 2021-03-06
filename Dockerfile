FROM quay.io/larsks/python:3.10

RUN pip install pipenv

WORKDIR /app
COPY Pipfile* /app/
RUN pipenv install
COPY . /app

ENTRYPOINT ["pipenv", "run", "python", "json2prom.py"]
