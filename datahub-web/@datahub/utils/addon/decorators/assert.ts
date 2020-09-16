import { assert } from '@ember/debug';
import { get } from '@ember/object';

/**
 * Decorator to assert that a property is not undefined. This decorator will only works
 * in ember components as it hooks into 'didReceiveAttrs` hook.
 * @param object Class to decorate
 * @param propName Property name to decorate
 */
export const assertComponentPropertyNotUndefined = function(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  object: Record<string, any>,
  propName: keyof typeof object
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): any {
  const didReceiveAttrs = object.didReceiveAttrs;
  object.didReceiveAttrs = function(...args: Array<unknown>): void {
    if (didReceiveAttrs) {
      didReceiveAttrs.apply(this, args);
    }
    const value = get(this, propName);

    assert(`Property ${propName} must be defined`, typeof value !== 'undefined');
  };
};
