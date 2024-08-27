package com.linkedin.util;

public class Pair<S, T> {

  private final S _first;
  private final T _second;

  public Pair(S first, T second) {
    _first = first;
    _second = second;
  }

  public S getFirst() {
    return _first;
  }

  public S getKey() {
    return getFirst();
  }

  public T getSecond() {
    return _second;
  }

  public T getValue() {
    return getSecond();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Pair) {
      Pair other = (Pair) obj;
      return equals(_first, other._first) && equals(_second, other._second);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h1 = _first != null ? _first.hashCode() : 0;
    int h2 = _second != null ? _second.hashCode() : 0;
    return 31 * h1 + h2;
  }

  @Override
  public String toString() {
    return "(" + _first + ',' + _second + ')';
  }

  private static boolean equals(Object o1, Object o2) {
    if (o1 != null) {
      return o1.equals(o2);
    }
    return o2 == null;
  }

  /*convenient method*/
  public static <S, T> Pair<S, T> of(S first, T second) {
    return new Pair<>(first, second);
  }
}
