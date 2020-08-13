import { containerDataSource } from '@datahub/utils/api/data-source';
import { task } from 'ember-concurrency';
import { ETaskPromise } from 'concurrency';

class PretendComponent {
  didInsertElement(): void {}
  didUpdateAttrs(): void {}
}

@containerDataSource<TestForDecoratorsDataSourceComponent>('getContainerDataTask', ['urn'])
export default class TestForDecoratorsDataSourceComponent extends PretendComponent {
  assert?: Assert;

  message?: string;

  urn?: string;

  @task(function*(this: TestForDecoratorsDataSourceComponent): IterableIterator<Promise<void>> {
    this.assert && this.assert.ok(true, this.message);
  })
  getContainerDataTask!: ETaskPromise<void>;
}
