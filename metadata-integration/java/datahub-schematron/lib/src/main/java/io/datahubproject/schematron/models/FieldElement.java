/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.schematron.models;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;

@Data
public class FieldElement {
  private List<String> type;
  private List<String> schemaTypes;
  private String name;
  private DataHubType parentType;

  public FieldElement(
      List<String> type, List<String> schemaTypes, String name, DataHubType parentType) {
    this.type = type;
    this.schemaTypes = schemaTypes;
    this.name = name;
    this.parentType = parentType;
  }

  public FieldElement clone() {
    return new FieldElement(new ArrayList<>(type), new ArrayList<>(schemaTypes), name, parentType);
  }

  public String asString(boolean v2Format) {
    if (v2Format) {
      String typePrefix =
          type.stream()
              .map(innerType -> "[type=" + innerType + "]")
              .collect(Collectors.joining("."));
      return name != null ? typePrefix + "." + name : typePrefix;
    } else {
      return name != null ? name : "";
    }
  }
}
