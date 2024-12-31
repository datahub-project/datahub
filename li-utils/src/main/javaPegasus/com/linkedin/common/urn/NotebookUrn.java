package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public class NotebookUrn extends Urn {
  public static final String ENTITY_TYPE = "notebook";

  private final String _notebookTool;
  private final String _notebookId;

  public NotebookUrn(String notebookTool, String notebookId) {
    super(ENTITY_TYPE, TupleKey.create(notebookTool, notebookId));
    this._notebookTool = notebookTool;
    this._notebookId = notebookId;
  }

  public String getNotebookToolEntity() {
    return _notebookTool;
  }

  public String getNotebookIdEntity() {
    return _notebookId;
  }

  public static NotebookUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static NotebookUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'notebook'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new NotebookUrn(
              (String) key.getAs(0, String.class), (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static NotebookUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<NotebookUrn>() {
          public Object coerceInput(NotebookUrn object) throws ClassCastException {
            return object.toString();
          }

          public NotebookUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return NotebookUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        NotebookUrn.class);
  }
}
