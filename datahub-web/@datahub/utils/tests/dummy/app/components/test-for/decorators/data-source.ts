import { containerDataSource } from '@datahub/utils/api/data-source';
import { Task, task } from 'ember-concurrency';

class PretendComponent {
  didInsertElement(): void {}
  didUpdateAttrs(): void {}
}

@containerDataSource('getContainerDataTask', ['urn'])
export default class TestForDecoratorsDataSourceComponent extends PretendComponent {
  assert?: Assert;

  message?: string;

  urn?: string;

  @task(function*(this: TestForDecoratorsDataSourceComponent): IterableIterator<Promise<void>> {
    this.assert && this.assert.ok(true, this.message);
  })
  getContainerDataTask!: Task<Promise<void>, () => Promise<void>>;
}
