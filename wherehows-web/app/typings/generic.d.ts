/**
 * Create a generic String-Union key -> value mapping constraint
 * where the string union must be keys on the map and values
 */
type StringUnionKeyToValue<U extends string> = { [K in U]: K };

export { StringUnionKeyToValue };
