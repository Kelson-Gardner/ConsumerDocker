This program will allow you to take widget objects that are being created by a producer.jar file that is either placing widgets in an S3 bucket or an SQS queue.

To run the program run this command:

To pull from a bucket that the widgets are in:

python3 consumer.py --inputs3 [bucket_input_name] [--s3 [name of s3 bucket to write to]] || [--dynamodb [name of dynamodb to write to]]

To pull from an SQS queue that the widgets are in:

python3 consumer.py --inputsqs [sqs queue url] [--s3 [name of s3 bucket to write to]] || [--dynamodb [name of dynamodb to write to]]

You can use either of the options --s3 or --dynamodb. Whichever argument you enter will determine where you put the widgets.
