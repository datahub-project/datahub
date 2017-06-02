package metadata.etl.models;

import java.util.Properties;
import metadata.etl.EtlJob;


public class DummyEtlJob extends EtlJob {

  public final long whExecId;
  public final Properties properties;

  public DummyEtlJob(int dbId, long whExecId, Properties prop) {
    super(null, null, whExecId, prop);
    this.whExecId = whExecId;
    this.properties = prop;
  }

  @Override
  public void extract() throws Exception {

  }

  @Override
  public void transform() throws Exception {

  }

  @Override
  public void load() throws Exception {

  }
}
