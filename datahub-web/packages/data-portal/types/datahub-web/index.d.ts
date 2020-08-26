import Ember from 'ember';

declare global {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix,@typescript-eslint/no-empty-interface
  interface Array<T> extends Ember.ArrayPrototypeExtensions<T> {}
  // interface Function extends Ember.FunctionPrototypeExtensions {}
}

export {};
