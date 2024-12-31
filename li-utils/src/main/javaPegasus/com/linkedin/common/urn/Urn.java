package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.util.ArgumentUtil;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

/**
 * Represents a URN (Uniform Resource Name) for a Linkedin entity, in the spirit of RFC 2141. Our
 * default URN format uses the non-standard namespace identifier "li", and hence default URNs begin
 * with "urn:li:". Note that the namespace according to <a
 * href="https://www.ietf.org/rfc/rfc2141.txt">RFC 2141</a> [Section 2.1] is case-insensitive and
 * for safety we only allow lower-case letters in our implementation.
 *
 * <p>Our URNs all consist of an "entity type", which denotes an internal namespace for the
 * resource, as well as an entity key, formatted as a tuple of parts. The full format of a URN is:
 *
 * <p>&lt;URN> ::= urn:&lt;namespace>:&lt;entityType>:&lt;entityKey>
 *
 * <p>The entity key is represented as a tuple of strings. If the tuple is of length 1, the key is
 * encoded directly. If the tuple has multiple parts, the parts are enclosed in parenthesizes and
 * comma-delimited, e.g., a URN whose key is the tuple [1, 2, 3] would be encoded as:
 *
 * <p>urn:li:example:(1,2,3)
 */
public class Urn {
  /**
   * @deprecated Don't create the Urn string manually, use Typed Urns or {@link #create(String
   *     entityType, Object... tupleParts)}
   */
  @Deprecated public static final String URN_PREFIX = "urn:li:";

  private static final String URN_START = "urn:";
  private static final String DEFAULT_NAMESPACE = "li";

  private final String _entityType;
  private final TupleKey _entityKey;
  private final String _namespace;

  // Used to speed up toString() in the common case where the Urn is built up
  // from parsing an input string.
  @Nullable private String _cachedStringUrn;

  static {
    Custom.registerCoercer(new UrnCoercer(), Urn.class);
  }

  /**
   * Customized interner for all strings that may be used for _entityType. Urn._entityType is by
   * nature a pretty small set of values, such as "member", "company" etc. Due to this fact, when an
   * app creates and keeps in memory a large number of Urn's, it may end up with a very big number
   * of identical strings. Thus it's worth saving memory by interning _entityType when an Urn is
   * instantiated. String.intern() would be a natural choice, but it takes a few microseconds, and
   * thus may become too expensive when many (temporary) Urns are generated in very quick
   * succession. Thus we use a faster CHM below. Compared to the internal table used by
   * String.intern() it has a bigger memory overhead per each interned string, but for a small set
   * of canonical strings it doesn't matter.
   */
  private static final Map<String, String> ENTITY_TYPE_INTERNER = new ConcurrentHashMap<>();

  /**
   * Create a Urn given its raw String representation.
   *
   * @param rawUrn - the String representation of a Urn.
   * @throws URISyntaxException - if the String is not a valid Urn.
   */
  public Urn(String rawUrn) throws URISyntaxException {
    ArgumentUtil.notNull(rawUrn, "rawUrn");
    _cachedStringUrn = rawUrn;

    if (!rawUrn.startsWith(URN_START)) {
      throw new URISyntaxException(rawUrn, "Urn doesn't start with 'urn:'. Urn: " + rawUrn, 0);
    }

    int secondColonIndex = rawUrn.indexOf(':', URN_START.length() + 1);
    _namespace = validateAndExtractNamespace(rawUrn, secondColonIndex);

    // First char of entityType must be [a-z]
    if (!charIsLowerCaseAlphabet(rawUrn, secondColonIndex + 1)) {
      throw new URISyntaxException(
          rawUrn, "First char of entityType must be [a-z]! Urn: " + rawUrn, secondColonIndex + 1);
    }

    int thirdColonIndex = rawUrn.indexOf(':', secondColonIndex + 2);

    // Case: urn:li:foo
    if (thirdColonIndex == -1) {
      _entityType = rawUrn.substring(secondColonIndex + 1);
      if (!charsAreWordClass(_entityType)) {
        throw new URISyntaxException(
            rawUrn, "entityType must have only [a-zA-Z0-9] chars. Urn: " + rawUrn);
      }
      _entityKey = new TupleKey();
      return;
    }

    String entityType = rawUrn.substring(secondColonIndex + 1, thirdColonIndex);
    if (!charsAreWordClass(entityType)) {
      throw new URISyntaxException(
          rawUrn, "entityType must have only [a-zA-Z_0-9] chars. Urn: " + rawUrn);
    }

    int numEntityKeyChars = rawUrn.length() - (thirdColonIndex + 1);
    if (numEntityKeyChars <= 0) {
      throw new URISyntaxException(
          rawUrn, "Urns with empty entityKey are not allowed. Urn: " + rawUrn);
    }

    _entityType = internEntityType(entityType);
    _entityKey = TupleKey.fromString(rawUrn, thirdColonIndex + 1);

    // For the sake of backwards compatibility, we must ensure that
    //   new Urn("urn:li:y:(urn:li:z:1)").toString() == "urn:li:y:urn:li:z:1"
    // Thus, if we detect a TupleKey with 1 part AND we had a paren in the
    // input, we abort our optimization of storing the original URN.
    if (_entityKey.size() == 1 && rawUrn.charAt(thirdColonIndex + 1) == '(') {
      _cachedStringUrn = null;
    }
  }

  /**
   * Create a Urn from an entity type and an encoded String key. The key is converted to a Tuple by
   * parsing using @see TupleKey#fromString
   *
   * @param entityType - the entity type for the Urn
   * @param typeSpecificString - the encoded string representation of a TupleKey
   * @throws URISyntaxException if the typeSpecificString is not a valid encoding of a TupleKey
   */
  public Urn(String entityType, String typeSpecificString) throws URISyntaxException {
    this(DEFAULT_NAMESPACE, entityType, TupleKey.fromString(typeSpecificString));
  }

  public Urn(String entityType, TupleKey entityKey) {
    this(DEFAULT_NAMESPACE, entityType, entityKey);
  }

  public Urn(String namespace, String entityType, TupleKey entityKey) {
    _namespace = namespace;
    _entityType = entityType;
    _entityKey = entityKey;
    _cachedStringUrn = null;
  }

  /**
   * DEPRECATED - use {@link #createFromTuple(String, Object...)} Create a Urn from an entity type
   * and a sequence of key parts. The key parts are converted to a tuple using @see TupleKey#create
   *
   * @param entityType - the entity type for the Urn
   * @param tupleParts - a sequence of objects representing the key of the Urn
   * @return - a new Urn object
   */
  @Deprecated
  public static Urn create(String entityType, Object... tupleParts) {
    return new Urn(entityType, TupleKey.create(tupleParts));
  }

  /**
   * DEPRECATED - use {@link #createFromTuple(String, java.util.Collection)} Create a Urn from an
   * entity type and a sequence of key parts. The key parts are converted to a tuple using @see
   * TupleKey#create
   *
   * @param entityType - the entity type for the Urn
   * @param tupleParts - a sequence of objects representing the key of the Urn
   * @return - a new Urn object
   */
  @Deprecated
  public static Urn create(String entityType, Collection<?> tupleParts) {
    return new Urn(entityType, TupleKey.create(tupleParts));
  }

  /**
   * Create a Urn from an entity type and a sequence of key parts. The key parts are converted to a
   * tuple using @see TupleKey#create
   *
   * @param entityType - the entity type for the Urn
   * @param tupleParts - a sequence of objects representing the key of the Urn
   * @return - a new Urn object
   */
  public static Urn createFromTuple(String entityType, Object... tupleParts) {
    return new Urn(entityType, TupleKey.create(tupleParts));
  }

  /**
   * Create a Urn from an namespace, entity type and a sequence of key parts. The key parts are
   * converted to a tuple using @see TupleKey#create
   *
   * @param namespace - The namespace of this urn.
   * @param entityType - the entity type for the Urn
   * @param tupleParts - a sequence of objects representing the key of the Urn
   * @return - a new Urn object
   */
  public static Urn createFromTupleWithNamespace(
      String namespace, String entityType, Object... tupleParts) {
    return new Urn(namespace, entityType, TupleKey.create(tupleParts));
  }

  /**
   * Create a Urn from an entity type and a sequence of key parts. The key parts are converted to a
   * tuple using @see TupleKey#create
   *
   * @param entityType - the entity type for the Urn
   * @param tupleParts - a sequence of objects representing the key of the Urn
   * @return - a new Urn object
   */
  public static Urn createFromTuple(String entityType, Collection<?> tupleParts) {
    return new Urn(entityType, TupleKey.create(tupleParts));
  }

  /**
   * Create a Urn given its raw String representation.
   *
   * @param rawUrn - the String representation of a Urn.
   * @throws URISyntaxException - if the String is not a valid Urn.
   */
  public static Urn createFromString(String rawUrn) throws URISyntaxException {
    return new Urn(rawUrn);
  }

  /**
   * Create a Urn given its raw CharSequence representation.
   *
   * @param rawUrn - the Char Sequence representation of a Urn.
   * @throws URISyntaxException - if the String is not a valid Urn.
   */
  public static Urn createFromCharSequence(CharSequence rawUrn) throws URISyntaxException {
    ArgumentUtil.notNull(rawUrn, "rawUrn");
    return new Urn(rawUrn.toString());
  }

  /**
   * Create a Urn from an entity type and an encoded String key. The key is converted to a Tuple by
   * parsing using @see TupleKey#fromString
   *
   * @param entityType - the entity type for the Urn
   * @param typeSpecificString - the encoded string representation of a TupleKey
   * @throws URISyntaxException if the typeSpecificString is not a valid encoding of a TupleKey
   */
  public static Urn createFromTypeSpecificString(String entityType, String typeSpecificString)
      throws URISyntaxException {
    return new Urn(entityType, typeSpecificString);
  }

  public String getEntityType() {
    return _entityType;
  }

  public String getNamespace() {
    return _namespace;
  }

  public TupleKey getEntityKey() {
    return _entityKey;
  }

  /**
   * Convenience method to get the key's first tuple element as a String
   *
   * @return key's first tuple element
   */
  public String getId() {
    return _entityKey.getAs(0, String.class);
  }

  /**
   * Convenience method to get the key's first tuple element as an Integer
   *
   * @return key's first tuple element, coerced to Integer
   */
  public Integer getIdAsInt() {
    return _entityKey.getAs(0, Integer.class);
  }

  /**
   * Convenience method to get the key's first tuple element as a Long
   *
   * @return key's first tuple element, coerced to Long
   */
  public Long getIdAsLong() {
    return _entityKey.getAs(0, Long.class);
  }

  public Urn getIdAsUrn() {
    return _entityKey.getAs(0, Urn.class);
  }

  /**
   * Return the namespace-specific string portion of this URN, i.e., everything following the
   * "urn:&lt;namespace>:" prefix.
   *
   * @return The namespace-specific string portion of this URN
   */
  public String getNSS() {
    return _entityType + (_entityKey.size() > 0 ? ':' + _entityKey.toString() : "");
  }

  @Override
  public String toString() {
    if (_cachedStringUrn != null) {
      return _cachedStringUrn;
    }
    // This can be written to by multiple threads, but that's actually safe
    // because Urn is immutable and all the threads will compute the same
    // logical String (even though they may produce different String objects).
    // So whichever thread "wins" the write race, the result is the same.
    // This field also doesn't need to be volatile for memory visibility
    // because it's just a cache, so if one thread sees a null here while
    // another sees non-null, it's still fine: the thread seeing non-null
    // uses the cache and the other thread computes a "new" value for the
    // field which is again the same logical String.
    _cachedStringUrn = URN_START + _namespace + ':' + getNSS();
    return _cachedStringUrn;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !Urn.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    Urn other = (Urn) obj;
    return _entityType.equals(other._entityType)
        && _entityKey.equals(other._entityKey)
        && _namespace.equals(other._namespace);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = _entityType.hashCode();
    result = prime * result + _entityKey.hashCode();
    return result;
  }

  private static String validateAndExtractNamespace(String rawUrn, int secondColonIndex)
      throws URISyntaxException {
    if (!charIsLowerCaseAlphabet(rawUrn, URN_START.length())) {
      throw new URISyntaxException(
          rawUrn, "First char of Urn namespace must be [a-z]! Urn: " + rawUrn, URN_START.length());
    }

    if (secondColonIndex == -1) {
      throw new URISyntaxException(rawUrn, "Missing second ':' char. Urn: " + rawUrn);
    }

    int namespaceLen = secondColonIndex - URN_START.length();
    if (namespaceLen > 32) {
      throw new URISyntaxException(
          rawUrn, "Namespace length > 32 chars. Urn: " + rawUrn, secondColonIndex);
    }

    if (namespaceLen == 2
        && rawUrn.charAt(URN_START.length()) == 'l'
        && rawUrn.charAt(URN_START.length() + 1) == 'i') {
      // We want to avoid an allocation for the ultra-common "li" namespace!
      return DEFAULT_NAMESPACE;
    }

    String namespace = rawUrn.substring(URN_START.length(), secondColonIndex);
    if (!charsAreValidNamespace(namespace)) {
      throw new URISyntaxException(rawUrn, "Chars in namespace must be [a-z0-9-]!. Urn: " + rawUrn);
    }
    return namespace;
  }

  // Not using Character.isLowerCase on purpose because that is unicode-aware
  // and we only need ASCII. Handling only ASCII is faster.
  private static boolean charIsLowerCaseAlphabet(String input, int index) {
    if (index >= input.length()) {
      return false;
    }
    char c = input.charAt(index);
    return c >= 'a' && c <= 'z';
  }

  // These are [a-z0-9-]
  private static boolean charsAreValidNamespace(String input) {
    for (int index = 0; index < input.length(); index++) {
      char c = input.charAt(index);
      // Not using Character.isLowerCase etc on purpose because that is
      // unicode-aware and we only need ASCII. Handling only ASCII is faster.
      if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) {
        return false;
      }
    }
    return true;
  }

  // Regex word class (\w) is defined as: [a-zA-Z_0-9]
  // Source: https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
  private static boolean charsAreWordClass(String input) {
    for (int index = 0; index < input.length(); index++) {
      char c = input.charAt(index);
      // Not using Character.isLowerCase etc on purpose because that is
      // unicode-aware and we only need ASCII. Handling only ASCII is faster.
      if (!((c >= 'a' && c <= 'z')
          || (c >= 'A' && c <= 'Z')
          || (c >= '0' && c <= '9')
          || c == '_')) {
        return false;
      }
    }
    return true;
  }

  /** Intern a string to be assigned to the _entityType field. */
  private static String internEntityType(String et) {
    // Most of the times this method is called, the canonical string is already
    // in the table, so let's do a quick get() first.
    String canonicalET = ENTITY_TYPE_INTERNER.get(et);
    if (canonicalET != null) {
      return canonicalET;
    }

    canonicalET = ENTITY_TYPE_INTERNER.putIfAbsent(et, et);
    return canonicalET != null ? canonicalET : et;
  }
}
