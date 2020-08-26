import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/concurrency-task-state-handler';
import { layout, tagName } from '@ember-decorators/component';
import { action } from '@ember/object';
import { Task, task } from 'ember-concurrency';

@layout(template)
@tagName('')
export default class ConcurrencyTaskStateHandler<T> extends Component {
  /**
   * List of arguments to be passed to the task when performed. The array is spread into the task invocation
   */
  taskArgs: Array<unknown> = [];

  /**
   * Flag indicating if the lockup image and retry button should be shown in the ui
   * Disabling allows this component to be used in interfaces that are not suitable to render a full error lockup
   */
  showLockup = true;

  /**
   * A flag indicating that the state handler should only handle async states and not the error state
   */
  handleAsyncStateOnly = false;

  /**
   * Ember concurrency task which is performed by this component on retry request, and the state derived from the task drives
   * component state. If the task is running a indication is shown in the ui, otherwise control is yielded to contextual content
   */
  task: Task<T, (...args: Array<unknown>) => unknown> = task(function*(): IterableIterator<T> {});

  /**
   * Action handler to perform the task component argument and pass externally supplied arguments to the task
   * @param {this['taskArgs']} args the arguments supplied as an array and spread into the task when performed / invoked
   */
  @action
  performTask(args: this['taskArgs']): void {
    this.task.perform(...args);
  }
}
