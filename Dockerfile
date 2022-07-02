FROM python:3.9-slim-buster

COPY ./requirements.txt /app/requirements.txt
WORKDIR /app

RUN pip install -r requirements.txt

COPY . /app

EXPOSE 5000

ENTRYPOINT [ "python3" ]
CMD ["app.py" ]