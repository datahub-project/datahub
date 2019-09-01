import Component from '@ember/component';
import { Task } from 'ember-concurrency';

/**
 * Returns a method decorator that cancels any inflight task instances for any supplied ember concurrency task names
 * This is for cases where it is needed to explicity cancel the task when the function is invoked
 * @template T
 * @param {...Array<string>} tasksToCancel the names of the task to be canceled
 * @returns {BabelMethodDecorator}
 */
export const cancelInflightTask = <T extends Component & Record<string, Task<unknown, unknown>>>(
  ...tasksToCancel: Array<string>
): ((_target: unknown, _method: unknown, prop: PropertyDescriptor) => PropertyDescriptor | undefined) => (
  _target: unknown,
  _method: keyof unknown,
  prop: PropertyDescriptor
): PropertyDescriptor | undefined => {
  const decoratedMethod = prop.value;
  if (decoratedMethod) {
    prop.value = function(this: T, ...args: Array<unknown>): unknown {
      // for each of the supplied Tasks, invoke the cancelAll method
      tasksToCancel.forEach((taskToCancel: string): void => this[taskToCancel].cancelAll());

      return decoratedMethod.apply(this, args);
    };
    return prop;
  }
  return undefined;
};
