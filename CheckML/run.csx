#r "Microsoft.WindowsAzure.Storage"
#load "..\Shared\MLDTO.csx"
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.ServiceBus.Messaging;
using System.Net.Http;
using System.Net.Http.Headers;

public static void Run(BrokeredMessage jobMsg, out BrokeredMessage queueMsg, TraceWriter log)
{
    try{
    Process(jobMsg, out queueMsg, log);
    }
    catch (Exception e){
        log.Info(e.ToString());
        queueMsg = null;
    }
}

public static void Process(BrokeredMessage jobMsg, out BrokeredMessage queueMsg, TraceWriter log)
 {
    log.Info("Message received");
    log.Info(jobMsg.ToString());
    var jobProperties = jobMsg.Properties;

    log.Info("Start listing keys:");
      foreach (string key in jobProperties.Keys) {
        log.Info( $"{key} : {jobProperties[key]}");
    }
    log.Info("Stop listing keys.");

    var jobId = (string)jobProperties["jobId"];
    var sourceLocalPath = (string)jobProperties["sourceLocalPath"];
    var sourceContainer = (string)jobProperties["sourceContainer"];

     queueMsg = null;

    //Hardcoded parameters
    var baseUrl = ConfigurationManager.AppSettings["mlBaseUrl"];
    var apiKey = ConfigurationManager.AppSettings["mlApiKey"];
    var apiVersion = "api-version=2.0";

    //Set up http client
    var httpClient = new HttpClient();
    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

    //Request job status
    var checkUrl =  $"{baseUrl}/{jobId}?{apiVersion}";
    var response = httpClient.GetAsync(checkUrl).Result;
    BatchScoreStatus status;
    //Handle failed request
    if (!response.IsSuccessStatusCode){
         log.Info("Check failed!");
         log.Info(response.ReasonPhrase);
         log.Info(response.Content.ReadAsStringAsync().Result);
         return;
     }
     //Handle successful request
     else{
         status = response.Content.ReadAsAsync<BatchScoreStatus>().Result;
         log.Info( $"Status is {status.ToString()}");
     }

    //Check if job is still running
     bool finished = !(
         status.StatusCode == BatchScoreStatusCode.NotStarted
         || status.StatusCode == BatchScoreStatusCode.Running );
    
    //Handle still running job (by putting back in queue)
    if (!finished){
        log.Info( $"Job not finished. Resubmitting...");
        var msg = new BrokeredMessage($"MLJOB::{sourceLocalPath}");
        msg.Properties.Add("jobId", jobId);
        msg.Properties.Add("sourceLocalPath", sourceLocalPath);
        msg.Properties.Add("sourceContainer", sourceContainer);
        msg.ScheduledEnqueueTimeUtc = DateTime.UtcNow.AddMinutes(0.1);
        queueMsg = msg;
        return;
    }
    else{
        queueMsg = null;
    }

    //Handle successful job
    if (status.StatusCode == BatchScoreStatusCode.Finished){
        log.Info("Job finished. Saving output ...");
        //Find ml output blob
        log.Info("Finding ML output blob...");
        AzureBlobDataReference blob = null;
        foreach (var output in status.Results)
            {
                blob = output.Value;
                break;
            }
        var credentials = new StorageCredentials(blob.SasBlobToken);
        var blobUri = new Uri(new Uri(blob.BaseLocation), blob.RelativeLocation);
        var outputCloudBlob = new CloudBlockBlob(blobUri, credentials);

        //Setup output blob
        log.Info("Setting up output blob...");
        CloudStorageAccount storageAccount = CloudStorageAccount.Parse( ConfigurationManager.AppSettings["fftestblob_STORAGE"] );
        CloudBlobClient outputClient = storageAccount.CreateCloudBlobClient();
        CloudBlobContainer outputContainer = outputClient.GetContainerReference(sourceContainer);
        var destinationBlobPath = sourceLocalPath.Replace("ds", "scored");
        var destinationBlob = outputContainer.GetBlockBlobReference(destinationBlobPath);

        log.Info($"Doing copy from {outputCloudBlob.Uri.LocalPath} to {destinationBlob.Uri.LocalPath} ...");
        var result = destinationBlob.StartCopyAsync(outputCloudBlob).Result;
        return;
    }
    //Handle unsuccessful job
    else{
        log.Info("Job failed: ");
        log.Info(status.Details);
    }

} 