import { noop } from 'lodash';

/**
 * Initializer currently only exists to create a named reference that other initializers can refer to.
 * For example, specifying order of initialization using the Ember attributes `before` / `after` in the exported object
 */
export const initialize = noop;

export default {
  initialize: noop
};
