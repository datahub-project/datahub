import Ember from 'ember';

// opt-in to allow types for Ember Array Prototype extensions
declare global {
  interface Array<T> extends Ember.ArrayPrototypeExtensions<T> {}
}
