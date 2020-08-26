/**
  Allows the specified non-null type to be nullable by creating a union type of null and type T
*/
export type Nullable<T> = T | null;

/**
 * extracted from https://medium.com/dailyjs/typescript-create-a-condition-based-subset-types-9d902cea5b8c
 *
 * Filters keys with a specific type
 *
 * For example:
 *
 * interface ISomething {
 *  a: string;
 *  b: number;
 * }
 * FilterKeys<ISomething, string> =>
 * {
 *  a: string;
 *  b: never
 * }
 *
 */
export type FilterKeys<T, ValueType> = {
  [Key in keyof T]: T[Key] extends ValueType ? Key : never;
};

/**
 * extracted from https://medium.com/dailyjs/typescript-create-a-condition-based-subset-types-9d902cea5b8c
 *
 * Return key names that match a specific type
 *
 * For example:
 *
 * interface ISomething {
 *  a: string;
 *  b: number;
 * }
 * KeyNamesWithValueType<ISomething, string> => 'a'
 */
export type KeyNamesWithValueType<T, ValueType> = FilterKeys<T, ValueType>[keyof T];

/**
 * Transforms a union type into a intersection type
 */
export type UnionToIntersection<U> = (U extends unknown
? (k: U) => void
: never) extends (k: infer I) => void
  ? I
  : never;

/**
 * Defines a constructable
 * @interface IConstructor
 * @template T
 */
export interface IConstructor<T> {
  new (...args: Array<unknown>): T;
}
