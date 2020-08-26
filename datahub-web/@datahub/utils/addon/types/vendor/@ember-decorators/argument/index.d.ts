/**
 * Preliminary type definitions for @ember-decorators/argument
 */
declare module '@ember-decorators/argument' {
  type Type = undefined | string | number | boolean | null | symbol | object;

  // https://github.com/ember-decorators/argument/blob/3edf6d105f3c5b3aeaabf68361795795f1183dfc/addon/-private/validators/-base.js#L26
  class BaseValidator {
    // https://github.com/ember-decorators/argument/blob/3edf6d105f3c5b3aeaabf68361795795f1183dfc/addon/-private/validators/-base.js#L27
    check: () => void;
    // https://github.com/ember-decorators/argument/blob/3edf6d105f3c5b3aeaabf68361795795f1183dfc/addon/-private/validators/-base.js#L31
    formatValue: () => string;
    // https://github.com/ember-decorators/argument/blob/3edf6d105f3c5b3aeaabf68361795795f1183dfc/addon/-private/validators/-base.js#L39
    run: () => void;
  }

  interface IArgument {
    <T extends BaseValidator | Type>(typeDefinition: T | (() => T)): PropertyDecorator;
  }

  export const argument: IArgument;
}

/**
 * Preliminary type definitions for @ember-decorators/argument/types
 */
declare module '@ember-decorators/argument/type' {
  import { argument, BaseValidator } from '@ember-decorators/argument';

  export const arrayOf: (...type: Parameters<typeof argument>) => BaseValidator;
  export const oneOf: (...typeArgs: Array<Parameters<typeof argument>>) => BaseValidator;
  export const optional: (...type: Parameters<typeof argument>) => BaseValidator;
  export const shapeOf: (type: object) => BaseValidator;
  export const unionOf: (...typeArgs: Array<Parameters<typeof argument>>) => BaseValidator;
}
