/**
 * Create a generic String-Union key -> value mapping constraint
 * where the string union must be keys on the map and values
 */
export type StringUnionKeyToValue<U extends string> = { [K in U]: K };

/**
 * Generic String Enum enforcing the keys on an object are found in
 * enum and value is of type V
 */
export type StringEnumKeyToEnumValue<T extends string, V> = { [K in T]: V };

/**
 * Describes the index signature for a generic object
 * @interface IObject
 */
export interface IObject<T> {
  [K: string]: T;
}

/**
 * Extracts the value of the a Record type
 * @template R
 * @alias {R extends Record<string, infer A> ? A : never}
 */
export type RecordValue<R extends Record<string, unknown>> = R extends Record<string, infer A> ? A : never;
