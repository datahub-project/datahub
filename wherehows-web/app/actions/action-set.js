import { assert } from '@ember/debug';

const actionSet = new Set();
const upperCaseUnderscoreRegex = /^[A-Z_]+$/g;

/**
 * Checks that actionType strings are unique and meet a minimum set of rules
 * @param {String} actionType a string representation of the action type
 * @return {String} actionType
 */
export default actionType => {
  assert(
    `Action types must be of type string and at least have a length of 1 char, ${actionType} does not satisfy this`,
    typeof actionType === 'string' && actionType.length
  );
  assert(
    `For consistency action types must contain only uppercase and underscore characters e.g ACTION_TYPE. ${actionType} does not`,
    String(actionType).match(upperCaseUnderscoreRegex)
  );
  assert(`The action type ${actionType} has already been previously registered`, !actionSet.has(actionType));
  actionSet.add(actionType);

  return actionType;
};
