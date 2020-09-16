// Ember changeset currently ships with incorrect types, hence the narrower, more accurate annotations below
// https://github.com/poteto/ember-changeset/issues/350
declare module 'ember-changeset' {
  import { UnwrapComputedPropertyGetter } from '@ember/object/-private/types';
  import EmberObject from '@ember/object';
  import Observable from '@ember/object/observable';

  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  export interface Config {
    skipValidate?: boolean;
  }

  export type ValidationOk = boolean | [boolean];
  export type ValidationErr = string | Array<string>;
  export type ValidationResult = ValidationOk | ValidationErr;

  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  export interface ValidatorFunc {
    (params: {
      key: string;
      newValue: unknown;
      oldValue: unknown;
      changes: {
        [key: string]: unknown;
      };
      content: object;
    }): ValidationResult | Promise<ValidationResult>;
  }

  /**
   * Creates new changesets.
   *
   * @uses Ember.Evented
   */
  export function changeset(
    obj: object,
    validateFn?: ValidatorFunc,
    validationMap?: {
      [s: string]: ValidatorFunc;
    },
    options?: Config
  ): Readonly<typeof EmberObject> & (new (properties?: object) => unknown) & (new (...args: Array<unknown>) => unknown);

  export default class Changeset<T> {
    rollback: Function;
    validate(key?: string): Promise<ValidationResult | null>;
    cast: Function;
    data: T;
    set<K extends keyof T, C extends keyof Changeset<T>>(
      key: K | C,
      value: T[K] | Changeset<T>[C]
    ): K extends keyof T ? T[K] : Changeset<T>[C];
    get<C extends keyof Changeset<T>>(key: C): UnwrapComputedPropertyGetter<Changeset<T>[C]>;
    get<K extends keyof T>(key: K): UnwrapComputedPropertyGetter<T[K]>;
    changes: Array<{ key: string; value: string }>;
    save: <T>(...args: Array<unknown>) => Promise<T>;
    isDirty: boolean;
    /**
     * Changeset factory
     *
     * @class Changeset
     * @constructor
     */
    constructor(
      obj: T,
      validateFn?: ValidatorFunc,
      validationMap?: {
        [s: string]: ValidatorFunc | Array<ValidatorFunc>;
      },
      options?: Config
    );
  }
}
