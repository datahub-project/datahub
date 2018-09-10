import { noop } from 'wherehows-web/utils/helpers/functions';
import { get } from '@ember/object';

/**
 * Invokes the container's task to fetch data on document insertion and successive attribute updates
 * @param {string} containerDataTaskName name o
 * @param {Array<any>} params optional list of arguments for the task generator function
 */
const containerDataSource = (containerDataTaskName: string, ...params: Array<any>): ClassDecorator => <
  T extends Function
>(
  klass: T
): T | void => {
  const { didInsertElement } = klass.prototype;
  const taskPerformer = (lifeCycleMethod: () => void) =>
    function(this: T, ...args: Array<any>) {
      // e-concurrency does not currently support dot notation access for CPs, hence Ember.get
      (<any>get(this, <Extract<keyof T, string>>containerDataTaskName)).perform(...params);
      lifeCycleMethod && lifeCycleMethod.apply(this, ...args);
    };

  klass.prototype.didInsertElement = taskPerformer(didInsertElement);
  klass.prototype.didUpdateAttrs = taskPerformer(noop);
};

export { containerDataSource };
