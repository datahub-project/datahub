import { helper } from '@ember/component/helper';
import { NachoTableComputedLink } from '@nacho-ui/core/types/nacho-table';
import { assert } from '@ember/debug';

export function nachoTableLinkCompute<T>([compute, rowData]: [
  (row: T) => NachoTableComputedLink,
  T
]): NachoTableComputedLink {
  assert('When computing a link, we expect a compute function to be provided', typeof compute === 'function');

  return compute(rowData);
}

export default helper(nachoTableLinkCompute);
