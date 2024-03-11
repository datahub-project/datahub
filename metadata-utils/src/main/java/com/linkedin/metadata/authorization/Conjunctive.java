package com.linkedin.metadata.authorization;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Collectors;

public class Conjunctive<E> extends LinkedList<E> {
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
