/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.authorization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class Conjunctive<E> extends ArrayList<E> {
  public static <E> Conjunctive<E> of(E... elements) {
    return new Conjunctive<>(Arrays.stream(elements).distinct().collect(Collectors.toList()));
  }

  public Conjunctive() {
    super();
  }

  public Conjunctive(Collection<? extends E> c) {
    super(c);
  }
}
