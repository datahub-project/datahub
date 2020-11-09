import Ember from 'ember';

// opt-in to allow types for Ember Array Prototype extensions
declare global {
  // eslint-disable-next-line @typescript-eslint/no-empty-interface, @typescript-eslint/interface-name-prefix
  interface Array<T> extends Ember.ArrayPrototypeExtensions<T> {}
}
