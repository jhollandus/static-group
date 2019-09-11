FROM python:3.6.9

RUN wget -qO - https://packages.confluent.io/deb/5.3/archive.key | apt-key add - \
    && echo "deb [arch=amd64] https://packages.confluent.io/deb/5.3 stable main" > /etc/apt/sources.list.d/confluent.list \
    && apt-get -y update \
    && apt-get -y install confluent-librdkafka-plugins

COPY . /app
WORKDIR /app

RUN pip install -e .

CMD ["static-consumer", "-c", "/config/static-consumer.json"]
