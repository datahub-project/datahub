package io.datahubproject.examples;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.StructuredPropertyParams;
import com.linkedin.metadata.aspect.patch.builder.FormInfoPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FormUpdate {

  private FormUpdate() {}

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    FormPrompt newPrompt =
        new FormPrompt()
            .setId("1234")
            .setTitle("First Prompt")
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setRequired(true)
            .setStructuredPropertyParams(
                new StructuredPropertyParams()
                    .setUrn(UrnUtils.getUrn("urn:li:structuredProperty:property1")));
    FormPrompt newPrompt2 =
        new FormPrompt()
            .setId("abcd")
            .setTitle("Second Prompt")
            .setType(FormPromptType.FIELDS_STRUCTURED_PROPERTY)
            .setRequired(false)
            .setStructuredPropertyParams(
                new StructuredPropertyParams()
                    .setUrn(UrnUtils.getUrn("urn:li:structuredProperty:property1")));

    Urn formUrn = UrnUtils.getUrn("urn:li:form:metadata_initiative_1");
    FormInfoPatchBuilder formInfoPatchBuilder =
        new FormInfoPatchBuilder()
            .urn(formUrn)
            .addPrompts(List.of(newPrompt, newPrompt2))
            .setName("Metadata Initiative 2024 (edited)")
            .setDescription("Edited description")
            .setOwnershipForm(true);
    MetadataChangeProposal mcp = formInfoPatchBuilder.build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
    Future<MetadataWriteResponse> response = emitter.emit(mcp, null);
    System.out.println(response.get().getResponseContent());
  }
}
