package io.datahub.test.action;

import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.RequiredArgsConstructor;


@Data
@RequiredArgsConstructor
public class ActionParameters {
  /**
   * The raw parameters provided to an {@link Action}.
   */
  private final Map<String, List<String>> params;
}
