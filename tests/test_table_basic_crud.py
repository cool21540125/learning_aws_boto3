import boto3
import pytest

URL = "http://localhost:8765"


@pytest.mark.timeout(0.5)
class TestBasicCRUD:

    c = boto3.resource("dynamodb", endpoint_url=URL)
    t = c.Table("demoCreateTable")

    def test_create_table(self):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
        """
        
        self.c.create_table(
            TableName="demoCreateTable",
            AttributeDefinitions=[
                {
                    "AttributeName": "kk",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "rr",
                    "AttributeType": "S"
                },
            ],
            KeySchema=[
                {
                    "AttributeName": "kk",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "rr",
                    "KeyType": "RANGE"
                },
            ],
            # LocalSecondaryIndexes=[],
            # GlobalSecondaryIndexes=[],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
            Tags=[
                {
                    'Key': 'usage',
                    'Value': 'learning'
                },
            ],
            TableClass='STANDARD',
        )

    def test_insert_item(self):
        self.t.put_item(
            Item={
                "kk": "1",
                "rr": "2"
            }
        )

    def test_update_item(self):
        self.t.update_item(
            Key={
                "kk": "1",
                "rr": "2"
            },
            UpdateExpression="SET age = :vall",
            ExpressionAttributeValues={
                ":vall" : "35"
            }
        )

    def test_get_item(self):
        res = self.t.get_item(
            Key={
                "kk": "1",
                "rr": "2"
            }
        )
        print(res["Item"])

    def test_delete_item(self):
        self.t.delete_item(
            Key={
                "kk": "1",
                "rr": "2"
            }
        )

    def test_delete_table(self):
        c = boto3.client("dynamodb", endpoint_url=URL)
        c.delete_table(TableName="demoCreateTable")
