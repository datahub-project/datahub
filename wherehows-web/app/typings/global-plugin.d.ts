import Ember from 'ember';

// opt-in to allow types for Ember Array Prototype extensions
declare global {
  // eslint-disable-next-line typescript/no-empty-interface, typescript/interface-name-prefix
  interface Array<T> extends Ember.ArrayPrototypeExtensions<T> {}
  type EmberTransition = Ember.Transition;
}
