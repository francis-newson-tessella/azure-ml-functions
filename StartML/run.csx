#r "Microsoft.WindowsAzure.Storage"
#r "Newtonsoft.Json"
#load "..\Shared\MLDTO.csx"
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.ServiceBus.Messaging;
using System.Net.Http;
using System.Net.Http.Headers;

public static void Run(CloudBlockBlob inputBlob, string name, out BrokeredMessage queueMsg, TraceWriter log)
{
    
    log.Info($"C# Blob trigger function Processed blob Name:{name} \n");

    //Hardcoded parameters
    string baseUrl = ConfigurationManager.AppSettings["mlBaseUrl"];
    string apiKey = ConfigurationManager.AppSettings["mlApiKey"];
    var apiVersion = "api-version=2.0";

    string connectionString = ConfigurationManager.AppSettings["fftestblob_STORAGE"];

    var permissions = SharedAccessBlobPermissions.Read;
    var policy = new SharedAccessBlobPolicy() {
        SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
        Permissions = permissions,
    };
 
    var inputBlobReference = new AzureBlobDataReference(){
       RelativeLocation =  inputBlob.Uri.LocalPath,
       BaseLocation =inputBlob.Container.ServiceClient.BaseUri.AbsoluteUri,
       SasBlobToken = inputBlob.GetSharedAccessSignature(policy)
    };

    inputBlobReference = new AzureBlobDataReference(){
        RelativeLocation =  inputBlob.Uri.LocalPath,
        ConnectionString = connectionString
    };

    log.Info( $"RelativeLocation {inputBlobReference.RelativeLocation}\n");
    log.Info( $"BaseLocation {inputBlobReference.BaseLocation}\n");
    log.Info( $"SasBlobToken {inputBlobReference.SasBlobToken}\n");

    var request = new BatchScoreRequest{
                Inputs = new Dictionary<string, AzureBlobDataReference>() {
                    { "input" , inputBlobReference }
                }
    };
     var submitUrl = $"{baseUrl}?{apiVersion}";
     var httpClient = new HttpClient();
     httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
     var response = httpClient.PostAsJsonAsync( submitUrl, request).Result;
     log.Info(Newtonsoft.Json.JsonConvert.SerializeObject(request));
     log.Info($"Submitted blob to {submitUrl}\n");
     if (!response.IsSuccessStatusCode){
         log.Info("Submission failed!");
         log.Info(response.ReasonPhrase);
         log.Info(response.Content.ReadAsStringAsync().Result);
     }
     //http://stackoverflow.com/q/19790588
     var jobId = response.Content.ReadAsAsync<string>().Result;
     
     log.Info($"Job ID is {jobId}\n");

    var startUrl = $"{baseUrl}/{jobId}/start?{apiVersion}";
    log.Info($"Starting at {startUrl}\n");
    response = httpClient.PostAsync( startUrl, null).Result;
    if (!response.IsSuccessStatusCode){
         log.Info("Submission failed!");
         log.Info(response.ReasonPhrase);
         log.Info(response.Content.ReadAsStringAsync().Result);
     }

    var checkUrl =  $"{baseUrl}/{jobId}?{apiVersion}";
    response = httpClient.GetAsync(checkUrl).Result;
    BatchScoreStatus status;
    if (!response.IsSuccessStatusCode){
         log.Info("Submission failed!");
         log.Info(response.ReasonPhrase);
         log.Info(response.Content.ReadAsStringAsync().Result);
     }
     else{
         status = response.Content.ReadAsAsync<BatchScoreStatus>().Result;
        log.Info( $"Status is {status}");
     }
    

    var msg = new BrokeredMessage($"MLJOB::{inputBlob.Uri.LocalPath}");
    msg.Properties.Add("jobId", jobId);
    msg.Properties.Add("sourceLocalPath", inputBlob.Name);
    msg.Properties.Add("sourceContainer", inputBlob.Container.Name);

    log.Info("Start listing keys:");
      foreach (string key in msg.Properties.Keys) {
        log.Info( $"{key} : {msg.Properties[key]}");
    }
    log.Info("Stop listing keys.");

    msg.ScheduledEnqueueTimeUtc = DateTime.UtcNow.AddMinutes(0.1);
    queueMsg = msg;
 }

 