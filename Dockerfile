FROM python:3.9

WORKDIR /code

COPY  ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

ENV PYTHONPATH "${PYTHONPATH}:/code/app"
# ENV AWS_ACCESS_KEY_ID "AKIAQONCNYAERM7KZPOD"
# ENV AWS_SECRET_ACCESS_KEY "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"

EXPOSE 80

CMD ["python", "./app/main.py"]
