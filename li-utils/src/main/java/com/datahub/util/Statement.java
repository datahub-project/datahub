package com.datahub.util;

import java.util.Collections;
import java.util.Map;
import lombok.NonNull;
import lombok.Value;

@Value
public class Statement {

  String commandText;

  Map<String, Object> params;

  public Statement(@NonNull String commandText, @NonNull Map<String, Object> params) {
    this.commandText = commandText;
    this.params = Collections.unmodifiableMap(params);
  }
}
