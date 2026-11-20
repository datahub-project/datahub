package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.springframework.validation.annotation.Validated;

/** Stream Governance association between a schema subject and a resource. */
@io.swagger.v3.oas.annotations.media.Schema(description = "Association")
@Validated
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Association {

  @JsonProperty("subject")
  private String subject = null;

  @JsonProperty("guid")
  private String guid = null;

  @JsonProperty("resourceName")
  private String resourceName = null;

  @JsonProperty("resourceNamespace")
  private String resourceNamespace = null;

  @JsonProperty("resourceId")
  private String resourceId = null;

  @JsonProperty("resourceType")
  private String resourceType = null;

  @JsonProperty("associationType")
  private String associationType = null;

  @JsonProperty("lifecycle")
  private String lifecycle = null;

  @JsonProperty("frozen")
  private Boolean frozen = null;

  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public String getGuid() {
    return guid;
  }

  public void setGuid(String guid) {
    this.guid = guid;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public String getResourceNamespace() {
    return resourceNamespace;
  }

  public void setResourceNamespace(String resourceNamespace) {
    this.resourceNamespace = resourceNamespace;
  }

  public String getResourceId() {
    return resourceId;
  }

  public void setResourceId(String resourceId) {
    this.resourceId = resourceId;
  }

  public String getResourceType() {
    return resourceType;
  }

  public void setResourceType(String resourceType) {
    this.resourceType = resourceType;
  }

  public String getAssociationType() {
    return associationType;
  }

  public void setAssociationType(String associationType) {
    this.associationType = associationType;
  }

  public String getLifecycle() {
    return lifecycle;
  }

  public void setLifecycle(String lifecycle) {
    this.lifecycle = lifecycle;
  }

  public Boolean getFrozen() {
    return frozen;
  }

  public void setFrozen(Boolean frozen) {
    this.frozen = frozen;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Association other = (Association) o;
    return Objects.equals(this.subject, other.subject)
        && Objects.equals(this.guid, other.guid)
        && Objects.equals(this.resourceName, other.resourceName)
        && Objects.equals(this.resourceNamespace, other.resourceNamespace)
        && Objects.equals(this.resourceId, other.resourceId)
        && Objects.equals(this.resourceType, other.resourceType)
        && Objects.equals(this.associationType, other.associationType)
        && Objects.equals(this.lifecycle, other.lifecycle)
        && Objects.equals(this.frozen, other.frozen);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        subject,
        guid,
        resourceName,
        resourceNamespace,
        resourceId,
        resourceType,
        associationType,
        lifecycle,
        frozen);
  }
}
