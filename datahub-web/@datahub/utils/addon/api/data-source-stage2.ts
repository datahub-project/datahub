import { getProperties, get } from '@ember/object';
import { isEqual } from '@ember/utils';
import { IParameterClassDescriptor } from '@datahub/utils/types/jan-2019-stage-2-decorators';

/**
 * Will compare 1 level down properties (return of getProperties);
 * @param previousProp reference to the old property by name
 * @param newProp new reference to the property with same name as previousProp
 */
const hasEqualProperties = <T extends Record<string, unknown>>(previousProp: T, newProp: T): boolean => {
  if (!previousProp || !newProp) {
    return false;
  }

  return Object.keys(previousProp).every((key: keyof T): boolean => isEqual(previousProp[key], newProp[key]));
};

type TaskPerformer = (...args: Array<unknown>) => void;

/**
 * Invokes the container's task to fetch data on document insertion and successive attribute updates.
 * Decorator function is curried
 * @template T
 * @param {keyof T} dataTaskName name of the task to call
 * @param {Array<keyof T>} attrs attributes to check for changes when component receives new / updated property changes
 * @param {...Array<any>} params optional parameters to supply to the task on invocation
 */
export const containerDataSourceStage2 = <T extends Record<string, unknown>>(
  dataTaskName: string,
  attrs: Array<keyof T>,
  ...params: Array<unknown>
): // eslint-disable-next-line @typescript-eslint/no-explicit-any
any =>
  /**
   * Decorates a container class to invoke the container's specified task property
   * @param {({ new (): T })} classDescriptor
   * @returns {DecoratorDescriptor}
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function(classDescriptor: IParameterClassDescriptor & any): any {
    const { kind, elements } = classDescriptor;
    if (kind !== 'class') {
      throw new Error('Expected a class descriptor');
    }

    /**
     * Performs the related task function
     * @param {() => void} lifeCycleMethod
     */
    const taskPerformer = (lifeCycleMethod?: string): TaskPerformer =>
      function(
        this: T & {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          _lastContainerDataSourceAttrs?: any;
          [K: string]: { perform: Function; apply: (...args: Array<unknown>) => unknown };
        },
        ...args: Array<unknown>
      ): void {
        const newAttrs = getProperties(this, attrs);
        const previousAttrs = this._lastContainerDataSourceAttrs;
        this._lastContainerDataSourceAttrs = newAttrs;

        if (!previousAttrs || !hasEqualProperties(previousAttrs, newAttrs)) {
          get(this, dataTaskName).perform(...params);
          lifeCycleMethod && this[lifeCycleMethod].apply(this, ...args);
        }
      };

    return {
      ...classDescriptor,
      elements: [
        ...elements,
        {
          key: 'didInsertElement',
          placement: 'prototype',
          kind: 'method',
          descriptor: {
            configurable: true,
            enumerable: false,
            writable: true,
            value: taskPerformer('didInsertElement')
          }
        },
        {
          key: 'didUpdateAttrs',
          placement: 'prototype',
          kind: 'method',
          descriptor: {
            configurable: true,
            enumerable: false,
            writable: true,
            value: taskPerformer()
          }
        }
      ]
    };
  };
