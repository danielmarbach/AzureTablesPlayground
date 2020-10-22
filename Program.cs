using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace AzureTablesPlayground
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var connectionString = File.ReadAllText("connectionstring.txt");
            var tableName = nameof(AzureTablesPlayground).ToLowerInvariant();
            var account = CloudStorageAccount.Parse(connectionString);
            var tableClient = account.CreateCloudTableClient();
            var table = tableClient.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync();

            await VerifyConcurrencyConflictBehavior(table);
            await VerifyMergeBehavior(table);
            await VerifyMergeBehavior(table, merge: true);

            Console.ReadLine();
        }

        private static async Task VerifyMergeBehavior(CloudTable table, bool merge = false) 
        {
            var partitionKey = Guid.NewGuid().ToString();
            var entityOneId = Guid.NewGuid().ToString();

            var entity1 = new SimilarEntity
            {
                PartitionKey = partitionKey,
                RowKey = entityOneId,
                Data = "DataNotModified",
                AnotherProperty = "Info we don't want to loose"
            };

            await table.ExecuteAsync(TableOperation.Insert(entity1));

            var entityRetrieveResult = await table.ExecuteAsync(TableOperation.Retrieve<Entity>(partitionKey, entityOneId));

            Console.WriteLine("Retrieve result:");
            Console.WriteLine(ObjectDumper.Dump(entityRetrieveResult, DumpStyle.Console));

            var entity1Retrieved = entityRetrieveResult.Result as Entity;
            entity1Retrieved.Data = "DataModified";

            if(merge) 
            {
                await table.ExecuteAsync(TableOperation.Merge(entity1Retrieved));
            }
            else {
                await table.ExecuteAsync(TableOperation.Replace(entity1Retrieved));
            }

            var entityRetrieveResultAfterModification = await table.ExecuteAsync(TableOperation.Retrieve(partitionKey, entityOneId));

            Console.WriteLine("Retrieve result after modification:");
            Console.WriteLine(ObjectDumper.Dump(entityRetrieveResultAfterModification, DumpStyle.Console));
        }

        private static async Task VerifyConcurrencyConflictBehavior(CloudTable table)
        {
            var partitionKey = Guid.NewGuid().ToString();
            var entityOneId = Guid.NewGuid().ToString();
            var entityTwoId = Guid.NewGuid().ToString();

            var entity1 = new Entity
            {
                PartitionKey = partitionKey,
                RowKey = entityOneId,
                Data = "DataNotModified"
            };

            var entity2 = new Entity
            {
                PartitionKey = partitionKey,
                RowKey = entityTwoId,
                Data = "DataNotModified"
            };

            var batch = new TableBatchOperation();
            batch.Add(TableOperation.Insert(entity1));
            batch.Add(TableOperation.Insert(entity2));

            var batchResult = await table.ExecuteBatchAsync(batch);
            Console.WriteLine("Batch result:");
            Console.WriteLine(ObjectDumper.Dump(batchResult, DumpStyle.Console));

            var entity1PreviousEtag = batchResult[0].Etag;
            var entity2PreviousEtag = batchResult[1].Etag;

            // modify entity 2
            entity2.Data = "DataModified";
            await table.ExecuteAsync(TableOperation.Replace(entity2));
            Console.WriteLine("Entity2 modified");

            entity1 = new Entity
            {
                PartitionKey = partitionKey,
                RowKey = entityOneId,
                Data = "DataModified",
                ETag = entity1PreviousEtag
            };

            entity2 = new Entity
            {
                PartitionKey = partitionKey,
                RowKey = entityTwoId,
                Data = "DataModifiedAgain",
                ETag = entity2PreviousEtag
            };

            batch.Clear();
            batch.Add(TableOperation.Replace(entity1));
            batch.Add(TableOperation.Replace(entity2));

            try {
                batchResult = await table.ExecuteBatchAsync(batch);
                Console.WriteLine("Batch result trying to modify again with previous etag:");
                Console.WriteLine(ObjectDumper.Dump(batchResult, DumpStyle.Console));
            }
            catch(StorageException ex) {
                Console.WriteLine("Exception from modification:");
                Console.WriteLine(ObjectDumper.Dump(ex, DumpStyle.Console));
            }

        }
    }

    class Entity : TableEntity 
    {
        public string Data { get; set;}
    }

    class SimilarEntity : TableEntity 
    {
        public string Data { get; set;}

        public string AnotherProperty { get; set;}
    }
}
