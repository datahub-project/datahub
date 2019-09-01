/**
 * Serializes the values in an array as a sorted string
 * @template S the type of values in the array that are assignable to type string
 * @param {Array<S>} stringArray array of values assignable to S
 * @returns {string}
 */
export const serializeStringArray = <S extends string>(stringArray: Array<S>): string =>
  String([...stringArray].sort());
