
public class DruidMetadataEtl extends EtlJob {
    public HdfsMetadataEtl(int dbId, long whExecId, Properties prop) {
        super(null, dbId, whExecId, prop);
    }
    public HdfsMetadataEtl(int dbId, long whExecId, Properties prop) {
        super(null, dbId, whExecId, prop);
    }

    @Override
    public void extractor() throws Exception {
        String url = "";
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);

        HttpPost post = new HttpPost(url);
        System.out.println("Running Druid Extractor!")

    }

    @Override
    public void transform() throws Exception {
        System.out.println("Running Druid Trasform!")
    }

    @Override
    public void load() throws Exception {
        System.out.println("Running Druid Load")
    }
}