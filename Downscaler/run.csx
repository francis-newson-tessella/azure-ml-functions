using CsvHelper;

public static void Run(Stream inputBlob, Stream outputBlob, string name, TraceWriter log)
{
    string _timeField = "Time";
    int _interval = 5;
    log.Info($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {inputBlob.Length} Bytes");

    //configure input reader
    var istream = new StreamReader(inputBlob);
    var csvReader = new CsvReader(istream);
    csvReader.Configuration.HasHeaderRecord = true;

    //configure output writer
    var ostream = new StreamWriter(outputBlob);
    var csvWriter = new CsvWriter(ostream);

    //write out headers
    csvReader.ReadHeader();
    ostream.WriteLine(String.Join(",", csvReader.FieldHeaders));

    //write out downscaled data
    while (csvReader.Read()){
         var dateField = csvReader.GetField<DateTime>(_timeField);
         if ((dateField.Second % _interval) != 0) {
              continue;
         }
         string[] record = csvReader.CurrentRecord;
         foreach (string field in record) {
             csvWriter.WriteField(field);
         }
         csvWriter.NextRecord();
    }
}