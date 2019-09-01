import { helper } from '@ember/component/helper';

type Params = [Record<string, unknown> | undefined, string];

/**
 * Will return the value of a property inside an object. The difference
 * between `get` and this helper is that, get won't work for properties with
 * '.' in the middle, like `something.something` inside this object:
 * {
 *  `something.something`: ...
 * }
 */
export function getFlat([object, key]: Params): unknown {
  return object && object[key];
}

export default helper(getFlat);
