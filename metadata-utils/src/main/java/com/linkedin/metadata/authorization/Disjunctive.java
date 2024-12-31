package com.linkedin.metadata.authorization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Disjunctive<E> extends ArrayList<E> {
  public static final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> DENY_ACCESS =
      new Disjunctive<>();

  public static <E> Disjunctive<Conjunctive<E>> disjoint(E... elements) {
    Disjunctive<Conjunctive<E>> result = new Disjunctive<>();
    result.addAll(
        Arrays.stream(elements).map(Conjunctive::<E>of).distinct().collect(Collectors.toList()));
    return result;
  }

  public static <E> Disjunctive<E> of(E... elements) {
    return new Disjunctive<>(Arrays.stream(elements).distinct().collect(Collectors.toList()));
  }

  public Disjunctive() {
    super();
  }

  public Disjunctive(Collection<? extends E> c) {
    super(c);
  }

  /**
   * For each distjoint conjunctive within a and b produce a combined disjunctive
   *
   * <p>a = [[priv1.1 && priv1.2] || [priv2.1 && priv2.2]] b = [[priv3.1 && priv3.2] || [priv4.1 &&
   * priv4.2]]
   *
   * <p>disjunctive of [ [priv1.1 && priv1.2 && priv3.1 && priv3.2] || [priv2.1 && priv2.2 &&
   * priv4.1 && priv4.2] || [priv2.1 && priv2.2 && priv3.1 && priv3.2] || [priv2.1 && priv2.2 &&
   * priv4.1 && priv4.2] ]
   */
  public static <E> Disjunctive<Conjunctive<E>> conjoin(
      Disjunctive<Conjunctive<E>> a, Disjunctive<Conjunctive<E>> b) {
    List<Conjunctive<E>> conjunctives = new LinkedList<>();
    for (Conjunctive<E> conjunctA : a) {
      for (Conjunctive<E> conjunctB : b) {
        conjunctives.add(
            new Conjunctive(
                Stream.concat(conjunctA.stream(), conjunctB.stream())
                    .distinct()
                    .collect(Collectors.toList())));
      }
    }
    return new Disjunctive<>(conjunctives.stream().distinct().collect(Collectors.toList()));
  }
}
