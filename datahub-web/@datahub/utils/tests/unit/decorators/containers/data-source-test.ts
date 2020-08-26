import { module, test } from 'qunit';
import TestForContainerDataSourceComponent from 'dummy/components/test-for/decorators/data-source';
import { set } from '@ember/object';

module('Unit | Utility | decorators/containers/data-source', function() {
  test('it properly adds data source to component', async function(assert): Promise<void> {
    const mockContainer = new TestForContainerDataSourceComponent();

    assert.expect(2);
    set(mockContainer, 'assert', assert);
    set(mockContainer, 'message', 'Runs data task on insert element');
    await mockContainer.didInsertElement();

    set(mockContainer, 'message', 'Runs data task on update attributes');
    set(mockContainer, 'urn', 'new urn');
    await mockContainer.didUpdateAttrs();
  });
});
