import Component from '@glimmer/component';
import { ETask } from '@datahub/utils/types/concurrency';
import { task } from 'ember-concurrency';

/**
 * Abstract Container will invoke a ember concurrency task. This simple class
 * will allow to create container classes that does not depend on ember-concurrency directly and
 * avoid simple boiler plate code
 */
export default abstract class EmberConcurrencyTaskContainer<T> extends Component<T> {
  /**
   * Will invoke initial container task
   * @param owner
   * @param args
   */
  constructor(owner: unknown, args: T) {
    super(owner, args);

    this.concurrencyTask.perform();
  }

  /**
   * containerInit EmberConcurrency wrapper
   */
  @task(function*(this: EmberConcurrencyTaskContainer<T>): IterableIterator<Promise<void>> {
    yield this.containerInit();
  })
  concurrencyTask!: ETask<void>;

  /**
   * Needs to be implmente with custom child logic
   */
  abstract containerInit(): Promise<void>;
}
