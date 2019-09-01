import { typeOf } from '@ember/utils';

/**
 * Checks if a type is an object
 * @param {any} candidate the entity to check
 */
// @ts-ignore
export const isObject = (candidate: unknown): candidate is object => typeOf(candidate) === 'object';
