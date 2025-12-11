/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.OBJECT_MAPPER;
import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.populateTopLevelKeys;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatch;
import java.io.StringReader;

public abstract class CompoundKeyTemplate<T extends RecordTemplate>
    implements ArrayMergingTemplate<T> {

  @Override
  public T applyPatch(RecordTemplate recordTemplate, JsonPatch jsonPatch)
      throws JsonProcessingException {
    JsonNode transformed = populateTopLevelKeys(preprocessTemplate(recordTemplate), jsonPatch);
    JsonObject patched =
        jsonPatch.apply(
            Json.createReader(new StringReader(OBJECT_MAPPER.writeValueAsString(transformed)))
                .readObject());
    JsonNode postProcessed = rebaseFields(OBJECT_MAPPER.readTree(patched.toString()));
    return RecordUtils.toRecordTemplate(getTemplateType(), postProcessed.toString());
  }
}
