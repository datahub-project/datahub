import { get, setProperties } from '@ember/object';

/**
 * Class Decorator that will set the properties of an 'options' object into 'this'.
 *
 * This behavior is useful when using helper {{component}} in this way:
 *
 * {{component name options=options}}
 *
 * Since there is no spread operator in HBS at the moment, we need to pass the options objects to the component.
 * If you want to reuse this component for other purposes, you may already have some attributes as regular params:
 *
 * <MyComponent @text="something" />
 *
 * In order to make this make this component compatible with this:
 *
 * <MyComponent @options={{hash text="something"}} />
 *
 * you need to adapt your component, but with this decorator, there is no change needed. Just add the decorator to the class
 * and it will be compatible both ways.
 *
 * @param propertyName defaults to 'options' but it can read other property names too.
 */
export const expandOptionsAttribute = function(propertyName = 'options'): ClassDecorator {
  return function(object: Function): void {
    const didReceiveAttrs = object.prototype.didReceiveAttrs;
    object.prototype.didReceiveAttrs = function(...args: Array<unknown>): void {
      if (didReceiveAttrs) {
        didReceiveAttrs.apply(this, args);
      }
      const value = get(this, propertyName);
      setProperties(this, value);
    };
  };
};
