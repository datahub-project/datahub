package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestInfo;
import java.util.List;
import org.mockito.ArgumentMatcher;

public class FormTestArgumentMatcher implements ArgumentMatcher<List<MetadataChangeProposal>> {

  private List<MetadataChangeProposal> leftList;

  public FormTestArgumentMatcher(List<MetadataChangeProposal> leftList) {
    this.leftList = leftList;
  }

  @Override
  public boolean matches(List<MetadataChangeProposal> rightList) {
    return rightList.stream().allMatch(right ->
        leftList.stream().anyMatch(left ->
            left.getEntityType().equals(right.getEntityType())
                && left.getAspectName().equals(right.getAspectName())
                && left.getChangeType().equals(right.getChangeType())
                && formTestsMatch(left.getAspect(), right.getAspect())));
  }

  private boolean formTestsMatch(GenericAspect left, GenericAspect right) {
    TestInfo leftProps =
        GenericRecordUtils.deserializeAspect(left.getValue(), "application/json", TestInfo.class);

    TestInfo rightProps =
        GenericRecordUtils.deserializeAspect(right.getValue(), "application/json", TestInfo.class);

    boolean defResult =
        formTestDefinitionsMatch(leftProps.getDefinition(), rightProps.getDefinition());
    if (!defResult) {
      return false;
    }

    // Verify other fields.
    return (leftProps.hasName() && leftProps.getName().equals(rightProps.getName()))
        && (leftProps.hasDescription()
            && leftProps.getDescription().equals(rightProps.getDescription()))
        && (leftProps.hasCategory() && leftProps.getCategory().equals(rightProps.getCategory()))
        && (leftProps.hasSource()
            && leftProps.getSource().hasType()
            && leftProps.getSource().getType().equals(rightProps.getSource().getType()))
        && (leftProps.hasSource()
            && leftProps.getSource().hasSourceEntity()
            && leftProps
                .getSource()
                .getSourceEntity()
                .equals(rightProps.getSource().getSourceEntity()));
  }

  private boolean formTestDefinitionsMatch(TestDefinition left, TestDefinition right) {
    // Simply decode the JSON and then compare each test json.
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode leftJsonNode = mapper.readTree(left.getJson());
      JsonNode rightJsonNode = mapper.readTree(right.getJson());
      return leftJsonNode.equals(rightJsonNode);
    } catch (Exception e) {
      throw new RuntimeException("Bad JSON found in test json");
    }
  }
}
