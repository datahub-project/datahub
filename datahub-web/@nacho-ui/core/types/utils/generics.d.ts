/**
 * @nacho-ui/core/utils/generics
 * This file contains the generic typings that can be used throughout nacho or any host application, meant
 * to add a little value and make life easier
 */

/**
 * Interface to easily declare an object where keys can be dynamic but all values will be of type T
 */
export interface IObject<T> {
  [key: string]: T;
}

/**
 * A very common object interface, where what is essentially a key value map is stored instead as an array of
 * objects with the interface { name: string, value: any }
 */
export interface IKeyMap<T> {
  name: string;
  value: T;
}

/**
 * Easy alias for a list of keymaps that maintain some similar expected typing between all values
 */
export type KeyMapList<T> = Array<IKeyMap<T>>;

/**
 * A common object that is used to store a generic object that may have a label to it and then a "data" field
 * to hold our list of data
 */
export interface IDataObject<T> {
  label?: string;
  data: Array<T>;
}

/**
 * A common object that takes the IDataObject and also gives it the ability to have children of the same interface.
 * This is helpful when parsing a heavily nested map into a tree of data objects for rendering purposes
 */
export interface IDataObjectTree<T> extends IDataObject<T> {
  children: Array<IDataObjectTree<T>>;
}
