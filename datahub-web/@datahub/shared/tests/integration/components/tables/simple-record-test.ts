import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import TablesSimpleRecord from '@datahub/shared/components/tables/simple-record';

module('Integration | Component | tables/simple-record', function(hooks): void {
  setupRenderingTest(hooks);

  const entries: TablesSimpleRecord['entries'] = {
    a: 1,
    b: 2,
    c: '3',
    d: '4',
    e: '5'
  };

  const title = 'Title';

  test('it renders entries', async function(assert): Promise<void> {
    this.setProperties({ entries, title });

    await render(hbs`
      <Tables::SimpleRecord
        @entries={{this.entries}}
        @title={{this.title}}
      />
    `);

    assert.dom().hasText('Title A 1 B 2 C 3 D 4 E 5');

    const updatedEntries = {
      anEntry: 'value'
    };
    this.set('entries', updatedEntries);

    await render(hbs`
    <Tables::SimpleRecord
      @entries={{this.entries}}
      @title={{this.title}}
    />
  `);

    assert.dom().hasText('Title An Entry value', 'Expected the first column letter casing to be titleized');
  });

  test('empty state behavior', async function(assert): Promise<void> {
    this.setProperties({ title });
    await render(hbs`<Tables::SimpleRecord @title={{this.title}} />`);

    assert.dom().hasText('Title We are unable to show any entries for this record');

    await render(hbs`<Tables::SimpleRecord @title={{this.title}} @includeTitleWhenEmpty={{false}} />`);

    assert.dom().hasText('We are unable to show any entries for this record');
  });
});
