/**
 * Create a generic String-Union key -> value mapping constraint
 * where the string union must be keys on the map and values
 */
type StringUnionKeyToValue<U extends string> = { [K in U]: K };

/**
 * Generic String Enum enforcing the keys on an object are found in
 * enum and value is of type V
 */
type StringEnumKeyToEnumValue<T extends string, V> = { [K in T]: V };

/**
 * Describes the index signature for a generic object
 * @interface IObject
 */
interface IObject<T> {
  [K: string]: T;
}

export { StringUnionKeyToValue, StringEnumKeyToEnumValue, IObject };
