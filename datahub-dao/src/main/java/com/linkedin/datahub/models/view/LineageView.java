package com.linkedin.datahub.models.view;

import lombok.Data;


@Data
public class LineageView {

  private DatasetView dataset;

  private String type;

  private String actor;

  private String modified;
}