/**
 * Deep clones a reference value provided. If the value is primitive, i.e. not immutable deepClone is an
 * identity function
 * @template T
 * @param {T} value
 * @return {T}
 */
const deepClone = <T>(value: T): T => (typeof value === 'object' ? JSON.parse(JSON.stringify(value)) : value);

export default deepClone;
