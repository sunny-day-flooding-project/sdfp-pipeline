FROM python:3.10.4

WORKDIR /code

COPY requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./scripts /code/scripts

EXPOSE 5432

ENTRYPOINT ["scripts/entrypoint.sh"]