/**
 * The field format that ties back to what kind of PII is found in the identifierType for a compliance annotation
 * @export
 * @namespace dataset
 * @enum {number}
 */
export enum FieldFormat {
  // Numerical format, 12345
  Numeric = 'NUMERIC',
  // URN format, urn:li:member:12345
  Urn = 'URN',
  // Reversed URN format, 12345:member:li:urn
  ReversedUrn = 'REVERSED_URN',
  // [Deprecated] Use CUSTOM format + pattern instead
  CompositeUrn = 'COMPOSITE_URN',
  // Any other non-standard format. A pattern for the value is expected to be provided
  Custom = 'CUSTOM',
  // Data is stored in reversible encoded/serialized/encrypted format
  Encoded = 'ENCODED',
  // Data is stored in irreversible hashed format
  Hashed = 'HASHED',
  // Any unencoded string-based field that is neither numeric nor a URN, e.g. alphanumeric strings, GUID etc.
  Raw = 'RAW'
}
