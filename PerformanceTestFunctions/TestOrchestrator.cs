using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace PerformanceTestFunctions
{
    public static class TestOrchestrator
    {
        public class QueueMessageModel
        {
            [JsonProperty("key")]
            public string Key { get; set; }
        }

        [FunctionName("TestOrchestrator")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context, ExecutionContext ctx, ILogger log)
        {
            var key = context.GetInput<QueueMessageModel>().Key;

            log.LogDebug($"*** Start Orch. : [Key] {key}");

            var retryOptions = new RetryOptions(
                firstRetryInterval: TimeSpan.FromSeconds(5),
                maxNumberOfAttempts: 3)
            {
                BackoffCoefficient = 3
            };

            string str = await context.CallActivityWithRetryAsync<string>("Activity_Hello", retryOptions, key);

            log.LogDebug($"*** Reach check point #1");

            var parallelTasks = new List<Task<object>>()
            {
                context.CallSubOrchestratorAsync<object>("TestSubOrchestrator", $"{str}A"),
                context.CallSubOrchestratorAsync<object>("TestSubOrchestrator", $"{str}B"),
                context.CallSubOrchestratorAsync<object>("TestSubOrchestrator", $"{str}C")
            };
            object[] result = await Task.WhenAll(parallelTasks);

            log.LogDebug($"*** Reach check point #2");

            await context.CallActivityWithRetryAsync<string>("Activity_Hello", retryOptions, result[0]);

            log.LogDebug($"*** End Orch.");
        }

        [FunctionName("TestSubOrchestrator")]
        public static async Task<string> RunSubOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context, ExecutionContext ctx, ILogger log)
        {
            var name = context.GetInput<string>();

            log.LogDebug($"**** Start SubOrch. : {name}");

            var retryOptions = new RetryOptions(
                firstRetryInterval: TimeSpan.FromSeconds(5),
                maxNumberOfAttempts: 3)
            {
                BackoffCoefficient = 3
            };

            var result = await context.CallActivityWithRetryAsync<string>("Activity_Hello", retryOptions, $"{name}#1");
            result = await context.CallActivityWithRetryAsync<string>("Activity_Hello", retryOptions, $"{result}#2");

            log.LogDebug($"*** Reach check point at SubOrch. : {name}");

            return result;
        }

        [FunctionName("Activity_Hello")]
        public static string Reply([ActivityTrigger] DurableActivityContext context, ILogger log)
        {
            var name = context.GetInput<string>();

            log.LogDebug($"***** Start Activity : {name}");

            return $"{name}";
        }

        [FunctionName("QueueStart")]
        public static async Task Run([QueueTrigger("durable-function-trigger")]QueueMessageModel myQueueItem,
            [OrchestrationClient]DurableOrchestrationClient starter, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed:  [Key] {myQueueItem.Key}");

            var instanceId = await starter.StartNewAsync("TestOrchestrator", myQueueItem);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
        }
    }
}