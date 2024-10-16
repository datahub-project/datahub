package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.FormType;
import com.linkedin.form.StructuredPropertyParams;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FormCreate {

  private FormCreate() {}

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    FormPromptArray prompts = new FormPromptArray();
    FormPrompt prompt1 =
        new FormPrompt()
            .setId("1")
            .setTitle("First Prompt")
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setRequired(true)
            .setStructuredPropertyParams(
                new StructuredPropertyParams()
                    .setUrn(UrnUtils.getUrn("urn:li:structuredProperty:property1")));
    FormPrompt prompt2 =
        new FormPrompt()
            .setId("2")
            .setTitle("Second Prompt")
            .setType(FormPromptType.FIELDS_STRUCTURED_PROPERTY)
            .setRequired(false)
            .setStructuredPropertyParams(
                new StructuredPropertyParams()
                    .setUrn(UrnUtils.getUrn("urn:li:structuredProperty:property1")));
    prompts.add(prompt1);
    prompts.add(prompt2);

    FormInfo formInfo =
        new FormInfo()
            .setName("Metadata Initiative 2024")
            .setDescription("Please respond to this form for metadata compliance purposes.")
            .setType(FormType.VERIFICATION)
            .setPrompts(prompts)
            .setActors(new FormActorAssignment().setOwners(true));

    MetadataChangeProposalWrapper mcpw =
        MetadataChangeProposalWrapper.builder()
            .entityType("form")
            .entityUrn("urn:li:form:metadata_initiative_1")
            .upsert()
            .aspect(formInfo)
            .aspectName("formInfo")
            .build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
    Future<MetadataWriteResponse> response = emitter.emit(mcpw, null);
    System.out.println(response.get().getResponseContent());
  }
}
