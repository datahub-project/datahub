/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openlineage.dataset;

import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Builder
@Getter
@Setter
@ToString
public class PathSpec {
  final String alias;
  final String platform;
  @Builder.Default final Optional<String> env = Optional.empty();
  final List<String> pathSpecList;
  @Builder.Default final Optional<String> platformInstance = Optional.empty();
}
