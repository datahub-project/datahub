import { helper } from '@ember/component/helper';
import { isArray } from '@ember/array';

export function listIncludes([list, item]: [Array<string | number>, string | number]): boolean {
  return isArray(list) && list.includes(item);
}

export default helper(listIncludes);
