import StorageArray from 'ember-local-storage/local/array';

/**
 * Creates an ArrayProxy class for persisting DataModel entities to localStorage
 * @export
 * @class Storage
 * @extends {StorageArray}
 */
export default class Storage<T> extends StorageArray<T> {
  static initialState<T>(): Array<T> {
    return [];
  }
}
