import { typeOf } from '@ember/utils';

/**
 * Checks if a type is an object
 * @param {any} candidate the entity to check
 */
// @ts-ignore https://github.com/typed-ember/ember-cli-typescript/issues/799
export const isObject = (candidate: unknown): candidate is object => typeOf(candidate) === 'object';
