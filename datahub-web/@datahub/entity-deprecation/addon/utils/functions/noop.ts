/**
 * Exports a noop that can be used in place of Ember.K which is currently deprecated.
 */
export default <(...args: Array<any>) => any>function noop(): any {};
