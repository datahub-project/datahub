package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentStatus;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Values that may be changed on an incident.
 *
 * <p>A null value means "not supplied" for a patch. The upsert operation deliberately treats null
 * values for nullable fields as clears instead. {@code startedAt} is only consulted by {@link
 * IncidentService#updateIncident}; {@link IncidentService#upsertIncident} never targets it, since
 * the editor does not own that field.
 */
public final class IncidentInfoUpdate {
  @Nullable private final String title;
  @Nullable private final String description;
  @Nullable private final Long startedAt;
  @Nullable private final IncidentStatus status;
  @Nullable private final Integer priority;
  @Nullable private final List<Urn> entities;
  @Nullable private final IncidentAssigneeArray assignees;

  private IncidentInfoUpdate(Builder builder) {
    this.title = builder.title;
    this.description = builder.description;
    this.startedAt = builder.startedAt;
    this.status = builder.status;
    this.priority = builder.priority;
    this.entities = builder.entities == null ? null : List.copyOf(builder.entities);
    this.assignees = builder.assignees;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Nullable
  public String getTitle() {
    return title;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  @Nullable
  public Long getStartedAt() {
    return startedAt;
  }

  @Nullable
  public IncidentStatus getStatus() {
    return status;
  }

  @Nullable
  public Integer getPriority() {
    return priority;
  }

  @Nullable
  public List<Urn> getEntities() {
    return entities;
  }

  @Nullable
  public IncidentAssigneeArray getAssignees() {
    return assignees;
  }

  public boolean isEmpty() {
    return title == null
        && description == null
        && startedAt == null
        && status == null
        && priority == null
        && entities == null
        && assignees == null;
  }

  public static final class Builder {
    @Nullable private String title;
    @Nullable private String description;
    @Nullable private Long startedAt;
    @Nullable private IncidentStatus status;
    @Nullable private Integer priority;
    @Nullable private List<Urn> entities;
    @Nullable private IncidentAssigneeArray assignees;

    private Builder() {}

    public Builder title(@Nullable String title) {
      this.title = title;
      return this;
    }

    public Builder description(@Nullable String description) {
      this.description = description;
      return this;
    }

    public Builder startedAt(@Nullable Long startedAt) {
      this.startedAt = startedAt;
      return this;
    }

    public Builder status(@Nullable IncidentStatus status) {
      this.status = status;
      return this;
    }

    public Builder priority(@Nullable Integer priority) {
      this.priority = priority;
      return this;
    }

    public Builder entities(@Nullable List<Urn> entities) {
      this.entities = entities;
      return this;
    }

    public Builder assignees(@Nullable IncidentAssigneeArray assignees) {
      this.assignees = assignees;
      return this;
    }

    public IncidentInfoUpdate build() {
      return new IncidentInfoUpdate(this);
    }
  }
}
