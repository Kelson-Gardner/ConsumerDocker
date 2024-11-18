import argparse
import json
import time
import boto3
import logging

logging.basicConfig(filename='consumer.log', level=logging.INFO)

class Consumer:
    def __init__(self, bucket_input, sqs_input, bucket_output, dynamodb_table):
        self.bucket_input = bucket_input
        self.sqs_input = sqs_input
        self.bucket_output = bucket_output
        self.dynamodb_table = dynamodb_table
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.sqs = boto3.client('sqs', region_name='us-east-1')

    def get_requests(self):
        
        if(self.bucket_input):
            response = self.s3.list_objects_v2(Bucket=self.bucket_input)
    
            if 'Contents' in response and response['Contents']:
                widget = response['Contents'][0]
                key = widget['Key']
                try:
                    response2 = self.s3.get_object(Bucket=self.bucket_input, Key=key)
                    
                    if response2['ContentLength'] > 0:
                        content = response2['Body'].read().decode('utf-8')
                        response2['Body'].close()
                        widget_request = json.loads(content)
                        print(widget_request)
                        print()
                        self.s3.delete_object(Bucket=self.bucket_input, Key=key)
                        return widget_request
                    else:
                        print(f"The object with key {key} is empty. No data to process.")
                        self.s3.delete_object(Bucket=self.bucket_input, Key=key)
                        return None
    
                except Exception as e:
                    logging.error(f"Error retrieving or processing {key}: {e}")
                    return None
            else:
                logging.info("No contents found in the bucket.")
                return None
        elif self.sqs_input:
            response = self.sqs.receive_message(
                QueueUrl=self.sqs_input,
                MaxNumberOfMessages=10)
            if 'Messages' in response:
                content = response['Messages'][0]['Body']
                try:
                    widget_request = json.loads(content)
                    self.sqs.delete_message(
                        QueueUrl=self.sqs_input,
                        ReceiptHandle=response['Messages'][0]['ReceiptHandle'])
                    print(widget_request)
                    return widget_request
                except:
                    return None

    def create_request(self, widget_request):
        widget_id = widget_request.get('widgetId')
        owner = widget_request.get('owner')
        widget_data = json.dumps(widget_request)
        if self.bucket_output:
            try:
                s3_key = f"widgets/{owner.replace(' ', '-').lower()}/{widget_id}"
                self.s3.put_object(Bucket=self.bucket_output, Key=s3_key, Body=widget_data)
                logging.info(f"Stored widget {widget_id} in S3 bucket {self.bucket_output} under key {s3_key}")
            except Exception as e:
                logging.error(f"Error storing widget in s3: {e}")
        elif self.dynamodb_table:
            self.store_in_dynamodb(widget_request)
            logging.info(f"Stored widget {widget_id} in DynamoDB table {self.dynamodb_table}")
        else:
            print("Invalid output storage value")
    
    def delete_request(self, widget_request):
        widget_id = widget_request.get('widgetId')
        owner = widget_request.get('owner')
        widget_data = json.dumps(widget_request)
        if self.bucket_output:
            try:
                s3_key = f"widgets/{owner.replace(' ', '-').lower()}/{widget_id}"
                self.s3.delete_object(Bucket=self.bucket_output, Key=s3_key)
                logging.info(f"Stored widget {widget_id} in S3 bucket {self.bucket_output} under key {s3_key}")
            except Exception as e:
                logging.error(f"Error storing widget in s3: {e}")
        elif self.dynamodb_table:
            self.store_in_dynamodb(widget_request)
            logging.info(f"Stored widget {widget_id} in DynamoDB table {self.dynamodb_table}")
        else:
            print("Invalid output storage value")
        
    def update_request(self, widget_request):
        widget_id = widget_request.get('widgetId')
        owner = widget_request.get('owner')
        widget_data = json.dumps(widget_request)
        if self.bucket_output:
            try:
                s3_key = f"widgets/{owner.replace(' ', '-').lower()}/{widget_id}"
                self.s3.put_object(Bucket=self.bucket_output, Key=s3_key)
                logging.info(f"Stored widget {widget_id} in S3 bucket {self.bucket_output} under key {s3_key}")
            except Exception as e:
                logging.error(f"Error storing widget in s3: {e}")
        elif self.dynamodb_table:
            self.store_in_dynamodb(widget_request)
            logging.info(f"Stored widget {widget_id} in DynamoDB table {self.dynamodb_table}")
        else:
            print("Invalid output storage value")

    def store_in_dynamodb(self, widget_request):
        table = self.dynamodb.Table(self.dynamodb_table)
        widget_request['id'] = str(widget_request['widgetId'])
        try:
            dynamodb_item = {
                "id": widget_request["widgetId"],
                "widgetId": widget_request["widgetId"],
                "owner": widget_request["owner"],
                "label": widget_request["label"],
                "description": widget_request["description"]
            }
        except Exception as e:
            logging.error(f"Error storing widget in DynamoDB: {e}")
        
        for attribute in widget_request.get("otherAttributes", []):
            dynamodb_item[attribute["name"]] = attribute["value"]
            
        try:
            table.put_item(Item=dynamodb_item)
        except Exception as e:
            logging.error(f"Error storing widget in DynamoDB: {e}")

    def run(self):
        while True:
            widget_request = self.get_requests()
            if widget_request:
                print(widget_request['type'])
                if widget_request['type'] == 'create':
                    self.create_request(widget_request)
                elif widget_request['type'] == 'delete':
                    self.delete_request(widget_request)
                elif widget_request['type'] == 'update':
                    self.update_request(widget_request)
            else:
                time.sleep(.1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputs3', required=False, help="Specify a bucket to read widget requests from")
    parser.add_argument('--inputsqs', required=False, help="Specify a SQS to read widget requests from")
    parser.add_argument('--s3', required=False, help="Specify storage target: s3")
    parser.add_argument('--dynamodb', required=False, help="Specify storage target: dynamodb")

    args = parser.parse_args()

    consumer = Consumer(args.inputs3, args.inputsqs, args.s3, args.dynamodb)
    consumer.run()
