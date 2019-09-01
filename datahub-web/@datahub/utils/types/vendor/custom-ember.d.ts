import ArrayPrototypeExtensions from '@ember/array/types/prototype-extensions';

// opt-in to allow types for Ember Array Prototype extensions
declare global {
  // eslint-disable-next-line @typescript-eslint/no-empty-interface, @typescript-eslint/interface-name-prefix
  interface Array<T> extends ArrayPrototypeExtensions<T> {}
}
