FROM python:3.11


COPY great-expectations great-expectations
RUN pip install -U pip
RUN pip install great_expectations==1.0.2
RUN pip install sqlalchemy
RUN pip install psycopg2
RUN pip install jupyter


WORKDIR /great-expectations
