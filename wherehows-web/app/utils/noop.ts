/**
 * Exports a noop that can be used in place of Ember.K which is currently deprecated.
 */
const noop: (...args: Array<any>) => any = () => {};
export default noop;
