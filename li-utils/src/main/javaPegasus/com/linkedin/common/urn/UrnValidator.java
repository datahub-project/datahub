package com.linkedin.common.urn;

import com.linkedin.data.message.Message;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.validator.Validator;
import com.linkedin.data.schema.validator.ValidatorContext;
import java.net.URISyntaxException;

/**
 * Rest.li Validator responsible for ensuring that {@link Urn} objects are well-formed.
 *
 * <p>Note that this validator does not validate the integrity of strongly typed urns, or validate
 * Urn objects against their associated key aspect.
 */
public class UrnValidator implements Validator {
  @Override
  public void validate(ValidatorContext context) {
    if (DataSchema.Type.TYPEREF.equals(context.dataElement().getSchema().getType())
        && ((NamedDataSchema) context.dataElement().getSchema()).getName().endsWith("Urn")) {
      try {
        Urn.createFromString((String) context.dataElement().getValue());
      } catch (URISyntaxException e) {
        context.addResult(
            new Message(
                context.dataElement().path(),
                "\"Provided urn %s\" is invalid",
                context.dataElement().getValue()));
        context.setHasFix(false);
      }
    }
  }
}
