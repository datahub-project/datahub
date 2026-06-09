// The V1 and V2 picker implementations were identical antd Select wrappers and stay identical
// after the alchemy rewrite. Both V1 and V2 callers feed the same `entity/shared/EntityContext`
// and the picker's data flow doesn't depend on any V1/V2-specific entity registry behaviour, so
// keep a single source of truth in V2 and re-export here.
export { default } from '@app/entityV2/shared/EntityDropdown/NodeParentSelect';
export { filterResultsForMove } from '@app/entityV2/shared/EntityDropdown/nodeParentSelectUtils';
