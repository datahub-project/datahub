package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class DataJobUrn extends Urn {

  public static final String ENTITY_TYPE = "dataJob";

  private final DataFlowUrn _flow;
  private final String _jobId;

  public DataJobUrn(DataFlowUrn flow, String jobId) {
    super(ENTITY_TYPE, TupleKey.create(flow, jobId));
    this._flow = flow;
    this._jobId = jobId;
  }

  public DataFlowUrn getFlowEntity() {
    return _flow;
  }

  public String getJobIdEntity() {
    return _jobId;
  }

  public static DataJobUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static DataJobUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'dataJob'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new DataJobUrn(
              (DataFlowUrn) key.getAs(0, DataFlowUrn.class), (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static DataJobUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.initializeCustomClass(DataFlowUrn.class);
    Custom.registerCoercer(
        new DirectCoercer<DataJobUrn>() {
          public Object coerceInput(DataJobUrn object) throws ClassCastException {
            return object.toString();
          }

          public DataJobUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return DataJobUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        DataJobUrn.class);
  }
}
