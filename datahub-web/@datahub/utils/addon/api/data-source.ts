import { get, getProperties } from '@ember/object';
import { isEqual } from '@ember/utils';
import { noop } from 'lodash';
import Component from '@ember/component';

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

/**
 * Invokes the container's task to fetch data on document insertion and successive attribute updates.
 * You need to specify which attributes can trigger fetch data task as a 2nd param.
 * @template T extends Component the component to augment with data source behavior
 * @param {string} containerDataTaskName name of the attribute on the container component that
 * references the ember-concurrency data fetching task to be invoked for component
 * life-cycle hooks didInsertElement & didUpdateAttrs
 * @param {Array<keyof T>} attrs attributes to 'watch' on didUpdateAttrs to perform task or not.
 * @param {Array<any>} params optional list of arguments for the task generator function
 */
export const containerDataSource = <T extends Pick<Component, 'didInsertElement' | 'didUpdateAttrs'>>(
  containerDataTaskName: keyof T,
  attrs: Array<keyof T>,
  // eslint-disable-next-line
  ...params: Array<any>
): ClassDecorator => (klass): void => {
  const taskPerformer = (lifeCycleMethod: () => void): (() => void) =>
    // eslint-disable-next-line
    function(this: T & { _lastContainerDataSourceAttrs?: any }, ...args: Array<any>) {
      const newAttrs = getProperties(this, attrs);
      const lastAttrs = this._lastContainerDataSourceAttrs;
      this._lastContainerDataSourceAttrs = newAttrs;

      if (!lastAttrs || !hasEqualProperties(lastAttrs, newAttrs)) {
        // e-concurrency does not currently support dot notation access for CPs, hence Ember.get
        // eslint-disable-next-line
        (<any>get(this, <Extract<keyof T, string>>containerDataTaskName)).perform(...params);
        lifeCycleMethod && lifeCycleMethod.apply(this, ...args);
      }
    };

  klass.prototype.didInsertElement = taskPerformer(klass.prototype.didInsertElement);
  klass.prototype.didUpdateAttrs = taskPerformer(noop);
};
