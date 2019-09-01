package com.linkedin.datahub.models;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PagedCollection<T> {

  private int total;

  private int start;

  private int count;

  private List<T> elements;
}
