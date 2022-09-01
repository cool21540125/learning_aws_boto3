import json
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
import pytest
from pprint import pprint as pp


URL = "http://localhost:8765"
TableName = "event_records"


@pytest.mark.timeout(0.5)
class TestBasicCRUD:

    c = boto3.resource("dynamodb", endpoint_url=URL)
    t = c.Table(TableName)

    def test_create_table(self):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
        """
        
        self.c.create_table(
            TableName=TableName,
            AttributeDefinitions=[
                {
                    "AttributeName": "event",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "date",
                    "AttributeType": "S"
                },
            ],
            KeySchema=[
                {
                    "AttributeName": "event",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "date",
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
        item = json.loads(json.dumps({
                "event": "body-health",
                "date": "2022-04-30",
                "weight": 111
            }), parse_float=Decimal)
        self.t.put_item(
            Item=item
        )

    def test_update_item(self):
        u = self.t.update_item(
            Key={
                "event": "body-health",
                "date": "2014-09-11"
            },
            UpdateExpression="SET weight = :vall",
            ExpressionAttributeValues={
                ":vall" : 69
            }
        )

    def test_get_item(self):
        res = self.t.get_item(
            Key={
                "event": "body-health",
                "date": "2014-09-11"
            }
        )
        print(res["Item"])

    def test_scan_with_conditions(self):
        # 慎用!! 耗費大量 RCU
        res = self.t.scan(
            FilterExpression=Attr('weight').lte(Decimal(70))
        )
        pp(res["Items"])

    def test_delete_item(self):
        self.t.delete_item(
            Key={
                "event": "body-health",
                "date": "2014-09-11"
            }
        )

    def test_query_item(self):
        res = self.t.query(
            KeyConditionExpression=Key("event").eq("body-health") & 
                Key("date").between("2018-01-01", "2021-12-31")
        )
        pp(res["Items"])

    def test_list_tables(self):
        c = boto3.client("dynamodb", endpoint_url=URL)
        tbls = c.list_tables(Limit=10)["TableNames"]
        print(tbls)

    def test_delete_table(self):
        c = boto3.client("dynamodb", endpoint_url=URL)
        c.delete_table(TableName=TableName)
