using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace ErrorHandlingTestFunctions
{
    public static class TestOrchestrator
    {
        [FunctionName("TestOrchestrator")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {
            await context.CallActivityAsync<string>("TestOrchestrator_Hello", ("World", false));

            var retryOptions = new RetryOptions(
                firstRetryInterval: TimeSpan.FromSeconds(5),
                maxNumberOfAttempts: 3);

            var outputs = new List<Task<string>>();

            // with retry
            outputs.Add(context.CallSubOrchestratorWithRetryAsync<string>("TestSubOrchestrator", retryOptions, "Tokyo"));
            // no retry
            outputs.Add(context.CallSubOrchestratorAsync<string>("TestSubOrchestrator", "Seattle"));
            outputs.Add(context.CallSubOrchestratorAsync<string>("TestSubOrchestrator", "London"));
            var result = await Task.WhenAll(outputs);

            await context.CallActivityAsync<string>("TestOrchestrator_Hello", ("World x2", false));

            return null;
        }

        [FunctionName("TestSubOrchestrator")]
        public static async Task<string> RunSubOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context, ILogger log)
        {
            var name = context.GetInput<string>();

            var retryOptions = new RetryOptions(
                firstRetryInterval: TimeSpan.FromSeconds(5),
                maxNumberOfAttempts: 3);

            // Replace "hello" with the name of your Durable Activity Function.

            // with retry
            await context.CallActivityWithRetryAsync<string>("TestOrchestrator_Hello", retryOptions, (name, false));

            // no retry
            return await context.CallActivityAsync<string>("TestOrchestrator_Hello", ($"{name} x2", false));
        }

        [FunctionName("TestOrchestrator_Hello")]
        public static string SayHello([ActivityTrigger] DurableActivityContext context, ILogger log)
        {
            var (name, throwEx) = context.GetInput<(string, bool)>();

            if (throwEx)
            {
                throw new Exception($"Exception occurred at {name}.");
            }

            log.LogInformation($"Saying hello to {name}.");
            return $"Hello {name}!";
        }

        [FunctionName("TestOrchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("TestOrchestrator", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}