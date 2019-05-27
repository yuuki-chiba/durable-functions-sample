using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;

namespace MessageInserter
{
    public static class Function1
    {
        //NOTE: This class is same as PerformanceTestFunctions.TestOrchestrator.QueueMessageModel
        public class QueueMessageModel
        {
            [JsonProperty("key")]
            public string Key { get; set; }
        }

        [FunctionName("Function1")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("local.settings.json", true)
                .AddEnvironmentVariables()
                .Build();

            var imgAccount = CloudStorageAccount.Parse(config.GetConnectionString("AccessKey"));

            var queueClient = imgAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference("durable-function-trigger");

            await queue.CreateIfNotExistsAsync();

            for (var i = 0; i < 100; ++i)
            {
                var message = new QueueMessageModel()
                    { Key = $"test_{i}" };
                await queue.AddMessageAsync(new CloudQueueMessage(JsonConvert.SerializeObject(message)));
            }

            return (ActionResult)new OkObjectResult($"End");
        }
    }
}
