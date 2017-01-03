public class AzureBlobDataReference
    {
        public string ConnectionString { get; set; }
        public string RelativeLocation { get; set; }
        public string BaseLocation { get; set; }
        public string SasBlobToken { get; set; }
    }

        public class BatchScoreRequest
    {
        public IDictionary<string, AzureBlobDataReference> Inputs { get; set; }
        public IDictionary<string, string> GlobalParameters { get; set; }
        public IDictionary<string, AzureBlobDataReference> Outputs { get; set; }
    }

      public enum BatchScoreStatusCode
    {
        NotStarted,
        Running,
        Failed,
        Cancelled,
        Finished
    }

        public class BatchScoreStatus
    {
        public BatchScoreStatusCode StatusCode { get; set; }
        public IDictionary<string, AzureBlobDataReference> Results { get; set; }
        public string Details { get; set; }
    }