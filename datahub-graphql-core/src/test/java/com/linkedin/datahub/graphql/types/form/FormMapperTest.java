package com.linkedin.datahub.graphql.types.form;

import static com.linkedin.metadata.Constants.FORM_INFO_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.FormType;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import java.util.List;
import java.util.Map;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class FormMapperTest {

  private static final Urn FORM_URN = UrnUtils.getUrn("urn:li:form:test");

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @Test
  public void testPromptWithTypeFromNewerVersionIsLeftOut() {
    // Read after a rollback: a prompt whose type only a newer version defines is left out, and
    // the form keeps its other prompts.
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);

    Form form = FormMapper.map(null, response(formWithNewerPrompt()));

    assertEquals(form.getInfo().getPrompts().size(), 1);
    assertEquals(form.getInfo().getPrompts().get(0).getId(), "known");
  }

  @Test
  public void testPromptWithTypeFromNewerVersionStrictHandlingFails() {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    EntityResponse response = response(formWithNewerPrompt());

    expectThrows(IllegalArgumentException.class, () -> FormMapper.map(null, response));
  }

  private static FormInfo formWithNewerPrompt() {
    FormPrompt known = prompt("known");
    FormPrompt newer = prompt("newer");
    newer.data().put("type", "PROMPT_TYPE_FROM_NEWER_BUILD");
    return new FormInfo()
        .setName("form")
        .setType(FormType.COMPLETION)
        .setPrompts(new FormPromptArray(List.of(known, newer)));
  }

  private static FormPrompt prompt(String id) {
    return new FormPrompt().setId(id).setTitle(id).setType(FormPromptType.STRUCTURED_PROPERTY);
  }

  private static EntityResponse response(FormInfo info) {
    return new EntityResponse()
        .setUrn(FORM_URN)
        .setEntityName("form")
        .setAspects(
            new EnvelopedAspectMap(
                Map.of(
                    FORM_INFO_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(FORM_INFO_ASPECT_NAME)
                        .setValue(new Aspect(info.data())))));
  }
}
