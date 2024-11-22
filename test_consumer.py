import unittest
import json
from consumer import Consumer
import tracemalloc

class TestConsumer(unittest.TestCase):

    def setUp(self):
        self.bucket_input = 'usu-cs5250-kgardner-requests'
        self.sqs_input = 'https://sqs.us-east-1.amazonaws.com/576677714530/cs5250-requests'
        self.bucket_output = 'usu-cs5250-kgardner-web'
        self.dynamodb_table = 'widget'
        self.consumer = Consumer(self.bucket_input, self.sqs_input, self.bucket_output, self.dynamodb_table)

    def test_get_requests_success(self):

        widget_data = {
            'type': 'create',
            'requestId': '6f6121c7-14d6-4d0a-a431-9f1a6dde8808',
            'widgetId': '7f0bfd22-876f-42f2-937d-cdf048caec2a',
            'owner': 'Mary Matthews',
            'label': 'ET',
            'description': 'DYNJLGBLSLEZLEJECEVLLXUMNPSJAEYVNECKBQFIHPAOMCSVRHZINZWXDXQFOTXDVCAGSAYK',
            'otherAttributes': [
                {'name': 'color', 'value': 'blue'},
                {'name': 'size', 'value': '143'},
                {'name': 'size-unit', 'value': 'cm'},
                {'name': 'height', 'value': '379'},
                {'name': 'width-unit', 'value': 'cm'},
                {'name': 'length-unit', 'value': 'cm'},
                {'name': 'price', 'value': '31.84'},
                {'name': 'quantity', 'value': '650'},
                {'name': 'note', 'value': 'EFCQGMBMWWRVXGQUZXUFWSSOUSXZSFEDMGEQISTGTKRAOFAFFSZVJWTLJYPMZWGRSULXEZDSHOXQEMBQPGXBCWSJABNPNEDMTPFJZMXLBOHOJCHLBVGTUDBEJDMOWNQHTTIYHMVPYWSUXYUBPTY'}
            ]
        }
        
        widget_data_bytes = json.dumps(widget_data).encode('utf-8')
        self.consumer.s3.put_object(Bucket=self.consumer.bucket_input, Key="hello", Body=widget_data_bytes)

        widget_request = self.consumer.get_requests()

        self.assertIsNotNone(widget_request)
        self.assertEqual(widget_request['widgetId'], '7f0bfd22-876f-42f2-937d-cdf048caec2a')
        self.assertEqual(widget_request['owner'], 'Mary Matthews')


    def test_get_requests_no_contents(self):
        widget_request = self.consumer.get_requests()
        self.assertIsNone(widget_request)

    def test_create_request_s3_success(self):
        widget_data = {
            'type': 'create',
            'requestId': '6f6121c7-14d6-4d0a-a431-9f1a6dde8808',
            'widgetId': '7f0bfd22-876f-42f2-937d-cdf048caec2a',
            'owner': 'Mary Matthews',
            'label': 'ET',
            'description': 'DYNJLGBLSLEZLEJECEVLLXUMNPSJAEYVNECKBQFIHPAOMCSVRHZINZWXDXQFOTXDVCAGSAYK',
            'otherAttributes': [
                {'name': 'color', 'value': 'blue'},
                {'name': 'size', 'value': '143'},
                {'name': 'size-unit', 'value': 'cm'},
                {'name': 'height', 'value': '379'},
                {'name': 'width-unit', 'value': 'cm'},
                {'name': 'length-unit', 'value': 'cm'},
                {'name': 'price', 'value': '31.84'},
                {'name': 'quantity', 'value': '650'},
                {'name': 'note', 'value': 'EFCQGMBMWWRVXGQUZXUFWSSOUSXZSFEDMGEQISTGTKRAOFAFFSZVJWTLJYPMZWGRSULXEZDSHOXQEMBQPGXBCWSJABNPNEDMTPFJZMXLBOHOJCHLBVGTUDBEJDMOWNQHTTIYHMVPYWSUXYUBPTY'}
            ]
        }
        
        widget_data_bytes = json.dumps(widget_data).encode('utf-8')
        self.consumer.s3.put_object(Bucket=self.consumer.bucket_input, Key="hello", Body=widget_data_bytes)

        widget_request = self.consumer.get_requests()
        self.consumer.create_request(widget_data)
        
        s3_key = f"widgets/{'Mary Matthews'.replace(' ', '-').lower()}/7f0bfd22-876f-42f2-937d-cdf048caec2a"
        object = self.consumer.s3.get_object(Bucket=self.bucket_output, Key=s3_key)
        finalObject = object['Body'].read().decode('utf-8')
        object['Body'].close()
        self.assertEqual(widget_data_bytes.decode('utf-8'), finalObject)

    def test_create_request_dynamodb_success(self):
        widget_data = {
            'type': 'create',
            'requestId': '6f6121c7-14d6-4d0a-a431-9f1a6dde8808',
            'widgetId': '7f0bfd22-876f-42f2-937d-cdf048caec2a',
            'owner': 'Mary Matthews',
            'label': 'ET',
            'description': 'DYNJLGBLSLEZLEJECEVLLXUMNPSJAEYVNECKBQFIHPAOMCSVRHZINZWXDXQFOTXDVCAGSAYK',
            'otherAttributes': [
                {'name': 'color', 'value': 'blue'},
                {'name': 'size', 'value': '143'},
                {'name': 'size-unit', 'value': 'cm'},
                {'name': 'height', 'value': '379'},
                {'name': 'width-unit', 'value': 'cm'},
                {'name': 'length-unit', 'value': 'cm'},
                {'name': 'price', 'value': '31.84'},
                {'name': 'quantity', 'value': '650'},
                {'name': 'note', 'value': 'EFCQGMBMWWRVXGQUZXUFWSSOUSXZSFEDMGEQISTGTKRAOFAFFSZVJWTLJYPMZWGRSULXEZDSHOXQEMBQPGXBCWSJABNPNEDMTPFJZMXLBOHOJCHLBVGTUDBEJDMOWNQHTTIYHMVPYWSUXYUBPTY'}
            ]
        }
        
        self.consumer.bucket_output = None
        self.consumer.create_request(widget_data)
        
        table = self.consumer.dynamodb.Table('widgets') 
        
        response = table.get_item(Key={"id": '7f0bfd22-876f-42f2-937d-cdf048caec2a'})
        
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_store_in_dynamodb(self):
        widget_request = {
            'widgetId': '1',
            'owner': 'test_owner',
            'label': 'Test Widget',
            'description': 'This is a test widget',
            'otherAttributes': [{'name': 'color', 'value': 'blue'}]
        }

        self.consumer.store_in_dynamodb(widget_request)

        expected_item = {
            "id": "1",
            "widgetId": "1",
            "owner": "test_owner",
            "label": "Test Widget",
            "description": "This is a test widget",
            "color": "blue"
        }
        table = self.consumer.dynamodb.Table('widgets') 
        object = table.get_item(Key={'id': "1"})
        self.assertEqual(object['ResponseMetadata']['HTTPStatusCode'], 200)
        
    def test_get_from_sqs(self):
        self.consumer.bucket_input = None
        widget_data = '{"type": "create", "requestId": "09d40f2d-f0e4-4f0e-b88d-211f1674865b", "widgetId": "e191203c-230e-42fc-801f-1dd73a8de1b4", "owner": "Mary Matthews", "label": "RAXY", "description": "JGAJDEBLRPHYWPDQTTMXEZAIPSOHCXWLKIRVCCCU", "otherAttributes": [{"name": "height", "value": "835"}, {"name": "length", "value": "717"}, {"name": "rating", "value": "2.941616"}, {"name": "quantity", "value": "573"}, {"name": "note", "value": "HKATCBHKRZIXHJOLPLYJIQSGNFBDHRDJIYZNROUBKDSMMQZNRNEFKSPCQTQBBRWRDMGCOIOTACPEPYOPBMTEWETYYOONUFZOFGPPXWJZYVOS"}]}'

        self.consumer.sqs.send_message(
            QueueUrl=self.sqs_input,
            MessageBody=widget_data)
        response = self.consumer.get_requests()
        self.assertEqual(dict(response)['type'], 'create')

    def test_sqs_is_empty(self):
        response = self.consumer.get_requests()
        self.assertEqual(response, None)
        
    def test_delete_s3_request(self):
        widget_data = {
            'type': 'delete',
            'requestId': '6f6121c7-14d6-4d0a-a431-9f1a6dde8808',
            'widgetId': '7f0bfd22-876f-42f2-937d-cdf048caec2a',
            'owner': 'Mary Matthews',
            'label': 'ET',
            'description': 'DYNJLGBLSLEZLEJECEVLLXUMNPSJAEYVNECKBQFIHPAOMCSVRHZINZWXDXQFOTXDVCAGSAYK',
            'otherAttributes': [
                {'name': 'color', 'value': 'blue'},
                {'name': 'size', 'value': '143'},
                {'name': 'size-unit', 'value': 'cm'},
                {'name': 'height', 'value': '379'},
                {'name': 'width-unit', 'value': 'cm'},
                {'name': 'length-unit', 'value': 'cm'},
                {'name': 'price', 'value': '31.84'},
                {'name': 'quantity', 'value': '650'},
                {'name': 'note', 'value': 'EFCQGMBMWWRVXGQUZXUFWSSOUSXZSFEDMGEQISTGTKRAOFAFFSZVJWTLJYPMZWGRSULXEZDSHOXQEMBQPGXBCWSJABNPNEDMTPFJZMXLBOHOJCHLBVGTUDBEJDMOWNQHTTIYHMVPYWSUXYUBPTY'}
            ]
        }
        widget_data_bytes = json.dumps(widget_data).encode('utf-8')
        self.consumer.s3.put_object(Bucket=self.consumer.bucket_input, Key="hello", Body=widget_data_bytes)

        widget_request = self.consumer.get_requests()
        self.consumer.create_request(widget_data)

        s3_key = f"widgets/{'Mary Matthews'.replace(' ', '-').lower()}/7f0bfd22-876f-42f2-937d-cdf048caec2a"
        
        self.consumer.delete_request(widget_request)
        try:
            object = self.consumer.s3.get_object(Bucket=self.bucket_output, Key=s3_key)
            self.assertEqual(0, 1)
        except:
            self.assertEqual(1, 1)
            
    def test_delete_dynamodb_request(self):
        widget_request = {
            'widgetId': '1',
            'owner': 'test_owner',
            'label': 'Test Widget',
            'description': 'This is a test widget',
            'otherAttributes': [{'name': 'color', 'value': 'blue'}]
        }

        self.consumer.store_in_dynamodb(widget_request)
        
        table = self.consumer.dynamodb.Table('widgets') 
        try:
            object = table.delete_item(TableName='widgets', Key={'id': "1"})
            self.assertEqual(0, 1)
        except:
            self.assertEqual(1, 1)
        
        
if __name__ == '__main__':
    unittest.main()
