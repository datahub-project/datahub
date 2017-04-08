package models;

public class DatasetFieldEntity {

  private String identifierType;
  private String identifierField;
  private String logicalType;
  private Boolean isSubject;

  public DatasetFieldEntity() {
  }

  public String getIdentifierType() {
    return identifierType;
  }

  public void setIdentifierType(String identifierType) {
    this.identifierType = identifierType;
  }

  public String getIdentifierField() {
    return identifierField;
  }

  public void setIdentifierField(String identifierField) {
    this.identifierField = identifierField;
  }

  public String getLogicalType() {
    return logicalType;
  }

  public void setLogicalType(String logicalType) {
    this.logicalType = logicalType;
  }

  public Boolean getIsSubject() {
    return isSubject;
  }

  public void setIsSubject(Boolean isSubject) {
    this.isSubject = isSubject;
  }
}
