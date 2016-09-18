using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

using Microsoft.Extensions.Configuration;

namespace RemoteDynamoDb
{
    public class Program
    {
        private readonly AmazonDynamoDBClient client;

        public static void Main(string[] args)
        {
            var program = new Program();
            program.Demo().Wait();
        }

        public Program()
        {
            var config = new ConfigurationBuilder()
                .AddUserSecrets("RemoteDynamoDB")
                .Build();
            var accessKey = config["aws-access-key"];
            var secretKey = config["aws-secret-key"];
            client = BuildClient(accessKey, secretKey);
        }

        private AmazonDynamoDBClient BuildClient(string accessKey, string secretKey)
        {
            Console.WriteLine("Creating DynamoDB client...");
            var credentials = new BasicAWSCredentials(
                accessKey: accessKey,
                secretKey: secretKey);
            var config = new AmazonDynamoDBConfig
            {
                RegionEndpoint = RegionEndpoint.USWest2
            };
            return new AmazonDynamoDBClient(credentials, config);
        }

        public async Task Demo()
        {
            var description = await BuildOrDescribeTable();
            while (description == null || !TableStatus.ACTIVE.Equals(description.TableStatus))
            {
                Console.WriteLine($"Table not ready yet. Status: {description?.TableStatus}. Sleeping 500 ms.");
                Thread.Sleep(500);
                description = await DescribeTable();
            }
            Console.WriteLine($"Table status: {description.TableStatus}");
            await SaveItem();
            var loadedItem = await FetchItem();
            Console.WriteLine($"Item loaded. Description: {loadedItem["Description"].S}");
        }

        private async Task<Dictionary<string, AttributeValue>> FetchItem()
        {
            Console.WriteLine("About to fetch item '123' from the Widgets table...");
            var response = await client.GetItemAsync(
                tableName: "Widgets",
                key: new Dictionary<string, AttributeValue>
                {
                    {"WidgetId", new AttributeValue {S = "123"}}
                }
            );
            return response.Item;
        }

        private async Task SaveItem()
        {
            Console.WriteLine("About to save item '123' to the Widgets table...");
            await client.PutItemAsync(
                tableName: "Widgets",
                item: new Dictionary<string, AttributeValue>
                {
                    {"WidgetId", new AttributeValue {S = "123"}},
                    {"Description", new AttributeValue {S = "This is a widget."}}
                }
            );
        }

        private async Task<TableDescription> DescribeTable()
        {
            var request = new DescribeTableRequest
            {
                TableName = "Widgets"
            };
            try
            {
                var result = await client.DescribeTableAsync(request);
                return result.Table;
            }
            catch (ResourceNotFoundException)
            {
                return null;
            }
        }

        private async Task<TableDescription> BuildOrDescribeTable()
        {
            var request = new CreateTableRequest(
                tableName: "Widgets",
                keySchema: new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                        AttributeName = "WidgetId",
                        KeyType = KeyType.HASH
                    }
                },
                attributeDefinitions: new List<AttributeDefinition>
                {
                    new AttributeDefinition()
                    {
                        AttributeName = "WidgetId",
                        AttributeType = ScalarAttributeType.S
                    }
                },
                provisionedThroughput: new ProvisionedThroughput
                {
                    ReadCapacityUnits = 1,
                    WriteCapacityUnits = 1
                }
            );
            Console.WriteLine("Sending request to build Widgets table...");
            try
            {
                var result = await client.CreateTableAsync(request);
                Console.WriteLine("Table created.");
                return result.TableDescription;
            }
            catch (ResourceInUseException)
            {
                // Table already created, just describe it
                Console.WriteLine("Table already exists. Fetching description...");
                return await DescribeTable();
            }
        }
    }
}
