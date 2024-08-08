package com.linkedin.common.urn;

import com.linkedin.data.template.DataTemplateUtil;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents the entity key portion of a Urn, encoded as a tuple of Strings. A single-element tuple
 * is encoded simply as the value of that element. A tuple with multiple elements is encoded as a
 * parenthesized list of strings, comma-delimited.
 */
public class TupleKey {
  public static final char START_TUPLE = '(';
  public static final char END_TUPLE = ')';
  public static final char DELIMITER = ',';

  private List<String> _tuple;

  public TupleKey(String... tuple) {
    _tuple = Arrays.asList(checkStringsNotNull(tuple));
  }

  public TupleKey(List<String> tuple) {
    this(tuple, true);
  }

  /**
   * Constructs a {@code TupleKey} given a list of tuple parts.
   *
   * <p>When {@code calledFromExternal} is {@code false}, it means the constructor was called from
   * within this class, where we can ensure our implementation satisfies some constraints and skip
   * some work.
   *
   * <p>The work we skip is checking that no tuple parts are null and wrapping the list with an
   * unmodifiable view.
   *
   * <p>For context, an earlier performance optimization introduced from Guava the {@code
   * ImmutableList}, which gives both of that for free. Since then, we have encountered
   * complications with Guava (specifically, Hadoop at the time of this writing requires using Guava
   * 11 -- see LIHADOOP-44200). In order to resolve that with minimal effect, we copy this behavior
   * here.
   *
   * <p>Whether this optimization is meaningful can be examined later, if time is permitting, or
   * {@code List#copyOf} from JDK 10 can be used to recover the benefits more elegantly when it is
   * available for us to use.
   *
   * @param tuple tuple parts
   * @param calledFromExternal whether the constructions is invoked from outside of this class
   */
  private TupleKey(List<String> tuple, boolean calledFromExternal) {
    _tuple = calledFromExternal ? Collections.unmodifiableList(checkStringsNotNull(tuple)) : tuple;
  }

  // This constructor is intentionally made non-public and should only be
  // invoked by the convenient method createWithOneKeyPart.
  // The reason why the String-vararg overload is insufficient is because it
  // creates needless garbage in the case of a single element. That vararg
  // methods allocates an array for the call, then copies that into a list.
  private TupleKey(String oneElement) {
    if (oneElement == null) {
      throw new NullPointerException("Cannot create URN with null part.");
    }
    _tuple = Collections.singletonList(oneElement);
  }

  public static TupleKey createWithOneKeyPart(String input) {
    return new TupleKey(input);
  }

  /**
   * Create a tuple key from a sequence of Objects. The resulting tuple consists of the sequence of
   * String values resulting from calling .toString() on each object in the input sequence
   *
   * @param tuple - a sequence of Objects to be represented in the tuple
   * @return - a TupleKey representation of the object sequence
   */
  public static TupleKey create(Object... tuple) {
    List<String> parts = new ArrayList<String>(tuple.length);

    for (Object o : tuple) {
      if (o == null) {
        throw new NullPointerException("Cannot create a Urn from tuple with null parameter.");
      }

      String objString = o.toString();
      if (objString.isEmpty()) {
        throw new IllegalArgumentException("Cannot create a Urn from tuple with an empty value.");
      }
      parts.add(objString);
    }
    return new TupleKey(Collections.unmodifiableList(parts), false);
  }

  /**
   * Create a tuple key from a sequence of Objects. The resulting tuple consists of the sequence of
   * String values resulting from calling .toString() on each object in the input sequence
   *
   * @param tuple - a sequence of Objects to be represented in the tuple
   * @return - a TupleKey representation of the object sequence
   */
  public static TupleKey create(Collection<?> tuple) {
    List<String> parts = new ArrayList<String>(tuple.size());

    for (Object o : tuple) {
      if (o == null) {
        throw new NullPointerException("Cannot create a Urn from tuple with null parameter.");
      }
      parts.add(o.toString());
    }
    return new TupleKey(Collections.unmodifiableList(parts), false);
  }

  public String getFirst() {
    return _tuple.get(0);
  }

  public String get(int index) {
    return _tuple.get(index);
  }

  /**
   * Return a tuple element coerced to a specific type
   *
   * @param index - the index of the tuple element to be returned
   * @param clazz - the Class object for the return type. Must be String, Short, Boolean, Integer,
   *     Long, or an Enum subclass
   * @param <T> - the desired type for the returned object.
   * @return The specified element of the tuple, coerced to the specified type T.
   */
  public <T> T getAs(int index, Class<T> clazz) {
    String value = get(index);

    Object result;

    if (value == null) {
      return null;
    } else if (String.class.equals(clazz)) {
      result = value;
    } else if (Short.TYPE.equals(clazz) || Short.class.equals(clazz)) {
      result = Short.valueOf(value);
    } else if (Boolean.class.equals(clazz) || Boolean.TYPE.equals(clazz)) {
      if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
        throw new IllegalArgumentException("Invalid boolean value: " + value);
      }
      result = Boolean.valueOf(value);
    } else if (Integer.TYPE.equals(clazz) || Integer.class.equals(clazz)) {
      result = Integer.valueOf(value);
    } else if (Long.TYPE.equals(clazz) || Long.class.equals(clazz)) {
      result = Long.valueOf(value);
    } else if (Enum.class.isAssignableFrom(clazz)) {
      result = getEnumValue(clazz, value);
    } else if (DataTemplateUtil.hasCoercer(clazz)) {
      result = DataTemplateUtil.coerceOutput(value, clazz);
    } else {
      throw new IllegalArgumentException("Cannot coerce String to type: " + clazz.getName());
    }
    @SuppressWarnings("unchecked")
    T rv = (T) result;
    return rv;
  }

  /** Helper method to capture E. */
  private <E extends Enum<E>> Enum<E> getEnumValue(Class<?> clazz, String value) {
    @SuppressWarnings("unchecked")
    final Class<E> enumClazz = (Class<E>) clazz.asSubclass(Enum.class);
    return Enum.valueOf(enumClazz, value);
  }

  public int size() {
    return _tuple.size();
  }

  public List<String> getParts() {
    return _tuple;
  }

  @Override
  public String toString() {
    if (_tuple.size() == 1) {
      return _tuple.get(0);
    } else {
      StringBuilder result = new StringBuilder();

      result.append(START_TUPLE);
      boolean delimit = false;
      for (String value : _tuple) {
        if (delimit) {
          result.append(DELIMITER);
        }
        result.append(value);
        delimit = true;
      }
      result.append(END_TUPLE);
      return result.toString();
    }
  }

  @Override
  public int hashCode() {
    return _tuple.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    return _tuple.equals(((TupleKey) obj)._tuple);
  }

  public static TupleKey fromString(String s) throws URISyntaxException {
    return new TupleKey(parseKeyParts(s, 0), false);
  }

  /**
   * Create a tuple key from a string starting at the given index.
   *
   * @param s raw urn string or urn type specific string.
   * @param startIndex index where urn type specific string starts.
   * @return entity tuple key.
   * @throws URISyntaxException if type specific string format is invalid.
   */
  public static TupleKey fromString(String s, int startIndex) throws URISyntaxException {
    return new TupleKey(parseKeyParts(s, startIndex), false);
  }

  private static List<String> parseKeyParts(String input, int startIndex)
      throws URISyntaxException {
    if (startIndex >= input.length()) {
      return Collections.emptyList();
    }

    // If there's no opening paren, there's only one tuple part. This is a very
    // common case so we special-case it for perf. We must still verify that
    // parens are balanced though.
    if (input.charAt(startIndex) != START_TUPLE) {
      if (!hasBalancedParens(input, startIndex)) {
        throw new URISyntaxException(input, "mismatched paren nesting");
      }
      return Collections.singletonList(input.substring(startIndex));
    }

    /* URNs with multiple-part ids overwhelmingly have just two or three parts.  As of May 5, a check of
     * existing typed URNs showed
     *
     *  890 single-part URN ids
     *  397 two-part URN ids
     *   86 three-part URN ids
     *   10 four-part URN ids
     *    1 five-part URN id
     *    1 seven-part URN id
     *
     * One-part URN ids should not even reach this point.
     * Specifying an initial capacity of three limits the wasted space for two-part URNs to one slot rather than
     * eight (as it would be for a default ArrayList capacity of 10) while providing enough slots for the 97.5%
     * of URN types which use three parts or fewer -- the rest will require some array expansion.
     */
    List<String> parts = new ArrayList<>(3);

    int numStartedParenPairs = 1; // We know we have at least one starting paren
    int partStart = startIndex + 1; // +1 to skip opening paren
    for (int i = startIndex + 1; i < input.length(); i++) {
      char c = input.charAt(i);
      if (c == START_TUPLE) {
        numStartedParenPairs++;
      } else if (c == END_TUPLE) {
        numStartedParenPairs--;
        if (numStartedParenPairs < 0) {
          throw new URISyntaxException(input, "mismatched paren nesting");
        }
      } else if (c == DELIMITER) {
        // If numStartedParenPairs == 0, then a comma is ignored because
        // we're not in parens. If numStartedParenPairs >= 2, we're inside an
        // nested paren pair and should also ignore the comma.
        // Don't forget: (foo,bar(zoo,moo)) parsed is ["foo", "bar(zoo,moo)"]!
        if (numStartedParenPairs != 1) {
          continue;
        }

        // Case: "(,,)" or "(,foo)" etc
        if (i - partStart <= 0) {
          throw new URISyntaxException(input, "empty part disallowed");
        }
        parts.add(input.substring(partStart, i));
        partStart = i + 1;
      }
    }

    if (numStartedParenPairs != 0) {
      throw new URISyntaxException(input, "mismatched paren nesting");
    }

    int lastPartEnd =
        input.charAt(input.length() - 1) == END_TUPLE ? input.length() - 1 : input.length();

    if (lastPartEnd - partStart <= 0) {
      throw new URISyntaxException(input, "empty part disallowed");
    }

    parts.add(input.substring(partStart, lastPartEnd));
    return Collections.unmodifiableList(parts);
  }

  private static boolean hasBalancedParens(String input, int startIndex) {
    int numStartedParenPairs = 0;
    for (int i = startIndex; i < input.length(); i++) {
      char c = input.charAt(i);
      if (c == START_TUPLE) {
        numStartedParenPairs++;
      } else if (c == END_TUPLE) {
        numStartedParenPairs--;
        if (numStartedParenPairs < 0) {
          return false;
        }
      }
    }
    return numStartedParenPairs == 0;
  }

  private static String[] checkStringsNotNull(String... array) {
    for (int i = 0; i < array.length; i++) {
      if (array[i] == null) {
        throw new NullPointerException("at index " + i);
      }
    }
    return array;
  }

  private static List<String> checkStringsNotNull(List<String> list) {
    int i = 0;
    for (String str : list) {
      if (str == null) {
        throw new NullPointerException("at index " + i);
      }
      i++;
    }
    return list;
  }
}
