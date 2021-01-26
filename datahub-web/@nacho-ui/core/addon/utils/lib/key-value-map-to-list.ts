import { IObject, IKeyMap } from '@nacho-ui/core/types/utils/generics';

/**
 * This function takes a key value map of interface { key: value } and turns it into a list of
 * key value maps, i.e. Array<{ name: key, value: value }>. Helpful when we want a list of the key
 * value pairs to iterate over in the template
 * @param kvMap - A map of key value pairs.
 * @param specifiedKeys - If we want to specify specific keys to be translated to the map list
 */
export default function keyValueMapToList<T>(kvMap: IObject<T> = {}, specifiedKeys?: Array<string>): Array<IKeyMap<T>> {
  return (specifiedKeys ? specifiedKeys : Object.keys(kvMap)).map(key => ({ name: key, value: kvMap[key] }));
}
