import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import getMockTableConfigs from 'dummy/tests/mock/table-configs';
import { DefaultTableClasses } from '@nacho-ui/table/constants/default-table-properties';

module('Integration | Component | nacho-table-body', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders for default config case', async function(assert) {
    await render(hbs`{{nacho-table-body}}`);
    assert.ok(true, 'Initial render is without errors');

    this.setProperties({
      fields: [
        { name: 'Bulbasaur', type: 'Grass', nature: 'Hardy' },
        { name: 'Charmander', type: 'Fire', nature: 'Timid' },
        { name: 'Squirtle', type: 'Water', nature: 'Impish' }
      ],
      tableConfigs: getMockTableConfigs()
    });

    await render(hbs`{{nacho-table-body
                       rows=fields
                       tableConfigs=tableConfigs
                      }}`);

    assert.equal(findAll(`.${DefaultTableClasses.row}`).length, 3, 'Renders 3 rows as expected');
  });
});
