using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
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
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context, ILogger log)
        {
            if (!context.IsReplaying) log.LogDebug($"*** Start Orch.");

            var retryOptions = new RetryOptions(
                firstRetryInterval: TimeSpan.FromSeconds(5),
                maxNumberOfAttempts: 2);
            retryOptions.Handle = (ex) =>
            {
                if (!context.IsReplaying) log.LogDebug($"*** Retry handling at Orch. : {ex.Message}");
                return true;
            };

            await context.CallActivityWithRetryAsync<string>("TestOrchestrator_Hello", retryOptions, ("World", false, false));

            if (!context.IsReplaying) log.LogDebug($"*** Reach check point #1");

            var outputs = new List<Task<string>>();

            // with retry
            outputs.Add(context.CallSubOrchestratorWithRetryAsync<string>("TestSubOrchestrator", retryOptions, "Tokyo"));
            outputs.Add(context.CallSubOrchestratorWithRetryAsync<string>("TestSubOrchestrator", retryOptions, "Seattle"));
            outputs.Add(context.CallSubOrchestratorWithRetryAsync<string>("TestSubOrchestrator", retryOptions, "London"));

            try
            {
                var result = await Task.WhenAll(outputs);
            }
            catch (Exception ex)
            {
                if (!context.IsReplaying) log.LogDebug($"*** Catch exception: {ex.Message}");
                // ignore exception
            }

            if (!context.IsReplaying) log.LogDebug($"*** Reach check point #2");

            await context.CallActivityAsync<string>("TestOrchestrator_Hello", ("World x2", false, false));

            if (!context.IsReplaying) log.LogDebug($"*** End Orch.");

            return "Succeeded";
        }

        [FunctionName("TestSubOrchestrator")]
        public static async Task<string> RunSubOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context, ILogger log)
        {
            var name = context.GetInput<string>();

            if (!context.IsReplaying) log.LogDebug($"**** Start SubOrch. : {name}");

            var retryOptions = new RetryOptions(
                firstRetryInterval: TimeSpan.FromSeconds(5),
                maxNumberOfAttempts: 2);
            retryOptions.Handle = (ex) =>
            {
                if (!context.IsReplaying) log.LogDebug($"*** Retry handling at SubOrch. : {ex.Message}");
                return true;
            };

            // variables for debug
            var throwEx = false;
            var useDelay = false;

            // Replace "hello" with the name of your Durable Activity Function.
            // with retry
            await context.CallActivityWithRetryAsync<string>("TestOrchestrator_Hello", retryOptions, (name, throwEx, useDelay));

            if (!context.IsReplaying) log.LogDebug($"*** Reach check point at SubOrch. : {name}");

            // variables for debug
            throwEx = false;
            useDelay = false;

            // timer setting for timeout
            TimeSpan timeout = TimeSpan.FromSeconds(15);
            DateTime deadline = context.CurrentUtcDateTime.Add(timeout);

            using (var cts = new CancellationTokenSource())
            {
                // no retry
                Task activityTask = context.CallActivityWithRetryAsync<string>("TestOrchestrator_Hello", retryOptions, ($"{name} x2", throwEx, useDelay));
                Task timeoutTask = context.CreateTimer(deadline, cts.Token);

                Task winner = await Task.WhenAny(activityTask, timeoutTask);
                if (winner == activityTask)
                {
                    if (!context.IsReplaying) log.LogDebug($"**** Task Complete at SubOrch. : {name}");

                    // success case
                    cts.Cancel();
                    return ((Task<string>)winner).Result;
                }
                else
                {
                    if (!context.IsReplaying) log.LogDebug($"**** Timeout at SubOrch. : {name}");

                    // timeout case
                    throw new Exception($"Timeout at {name}.");
                }
            }
        }

        [FunctionName("TestOrchestrator_Hello")]
        public static string SayHello([ActivityTrigger] DurableActivityContext context, ILogger log)
        {
            var (name, throwEx, useDelay) = context.GetInput<(string, bool, bool)>();

            log.LogDebug($"***** Start Activity : {name}");

            if (useDelay)
            {
                Thread.Sleep(TimeSpan.FromSeconds(30));
                //Thread.Sleep(TimeSpan.FromSeconds(330)); // over 5 minutes
            }

            if (throwEx)
            {
                log.LogDebug($"***** Exception at Activity : {name}");

                throw new Exception($"***** Exception occurred at {name}.");
            }

            log.LogInformation($"***** Saying hello to {name}.");
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

            log.LogInformation($"***** Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}