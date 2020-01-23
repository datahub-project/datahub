declare module 'ember-local-storage/local/object';

declare module 'ember-local-storage/local/array' {
  import NativeArray from '@ember/array/-private/native-array';
  import ArrayProxy from '@ember/array/proxy';

  export default class StorageArray<T> extends ArrayProxy<T> {
    isInitialContent: () => boolean;
    content: NativeArray<T>;
    reset: () => void;
    clear: () => this;
    replaceContent: () => void;
  }
}

declare module 'ember-local-storage' {
  import ComputedProperty from '@ember/object/computed';
  const storageFor: (key: string, modelName?: string, options?: {}) => ComputedProperty<unknown>;
  export { storageFor };
}
