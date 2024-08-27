package datahub.event;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DatasetProperties;
import java.net.URISyntaxException;
import org.junit.Assert;
import org.junit.Test;

public class MetadataChangeProposalWrapperTest {

  /** We should throw errors on validation as exceptions */
  @Test
  public void testBuilderExceptions() {
    try {
      MetadataChangeProposalWrapper.create(
          b -> b.entityType("dataset").entityUrn("foo") // bad urn should throw exception
          );
      Assert.fail("Should throw an exception");
    } catch (EventValidationException e) {
      Assert.assertTrue(
          "Underlying exception should be a URI syntax issue",
          e.getCause() instanceof URISyntaxException);
    } catch (Exception e) {
      Assert.fail("Should not throw any other exception");
    }
  }

  @Test
  public void testAspectInferenceSuccess() throws EventValidationException {
    MetadataChangeProposalWrapper mcpw =
        MetadataChangeProposalWrapper.create(
            b ->
                b.entityType("dataset")
                    .entityUrn("urn:li:dataset:(foo,bar,PROD)")
                    .upsert()
                    .aspect(new DatasetProperties()));
    Assert.assertEquals(mcpw.getAspectName(), "datasetProperties");
  }

  /**
   * We throw exceptions on using the regular builder pattern
   *
   * @throws URISyntaxException
   * @throws EventValidationException
   */
  @Test(expected = EventValidationException.class)
  public void testAspectInferenceFailure() throws URISyntaxException, EventValidationException {
    MetadataChangeProposalWrapper mcpw =
        MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn("urn:li:dataset:(foo,bar,PROD)")
            .upsert()
            .aspect(new AuditStamp().setActor(Urn.createFromString("urn:li:corpUser:jdoe")))
            .build();
  }

  /**
   * We throw exceptions on using the lambda builder pattern
   *
   * @throws URISyntaxException
   * @throws EventValidationException
   */
  @Test(expected = EventValidationException.class)
  public void testAspectInferenceFailureLambda()
      throws URISyntaxException, EventValidationException {
    Urn actorUrn = Urn.createFromString("urn:li:corpUser:jdoe");
    MetadataChangeProposalWrapper mcpw =
        MetadataChangeProposalWrapper.create(
            b ->
                b.entityType("dataset")
                    .entityUrn("urn:li:dataset:(foo,bar,PROD)")
                    .upsert()
                    .aspect(new AuditStamp().setActor(actorUrn)));
  }
}
