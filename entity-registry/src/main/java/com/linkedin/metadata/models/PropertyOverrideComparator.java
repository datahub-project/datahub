package com.linkedin.metadata.models;

import java.util.Comparator;
import org.apache.commons.lang3.tuple.Pair;

public class PropertyOverrideComparator implements Comparator<Pair<String, Object>> {
  public int compare(Pair<String, Object> o1, Pair<String, Object> o2) {
    return Integer.compare(o2.getKey().split("/").length, o1.getKey().split("/").length);
  }
}
