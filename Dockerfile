FROM python:3.10.4

WORKDIR /code

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . .

EXPOSE 5432

CMD ["python", "scripts/process_pressure.py"]