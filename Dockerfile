FROM python:3.12-alpine

RUN pip install boto3

COPY consumer.py consumer.py

CMD ["python3", "consumer.py", "--inputsqs", "https://sqs.us-east-1.amazonaws.com/576677714530/cs5250-requests", "--s3", "S3"]

