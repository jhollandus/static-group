FROM python:3.6.9-alpine
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD python ./consumer.py
