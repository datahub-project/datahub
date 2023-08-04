package io.datahub.test.query;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public abstract class BaseQueryEvaluator implements QueryEvaluator {
  @Getter
  @Setter
  QueryEngine queryEngine;
}
