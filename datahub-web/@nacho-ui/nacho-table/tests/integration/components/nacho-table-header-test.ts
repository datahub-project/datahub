import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import getMockTableConfigs from 'dummy/tests/mock/table-configs';
import { DefaultTableClasses } from '@nacho-ui/table/constants/default-table-properties';

module('Integration | Component | nacho-table-header', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders for a default config case', async function(assert) {
    await render(hbs`{{nacho-table-header}}`);
    assert.ok(true, 'Initial render is without errors');

    this.set('mockConfigs', getMockTableConfigs());

    await render(hbs`{{nacho-table-header
                       tableConfigs=mockConfigs}}`);

    assert.equal(findAll(`.${DefaultTableClasses.title}`).length, 3, 'Renders 3 default title items');
    assert.equal(
      ((find(`.${DefaultTableClasses.title}:nth-child(2)`) as Element).textContent || '').trim(),
      'Type',
      'Renders a column header correctly in the default case'
    );
  });

  test('it renders for a default component blocks case', async function(assert) {
    await render(hbs`{{#nacho-table-header as |header|}}
                       {{#header.title}}
                         Charmander
                       {{/header.title}}
                       {{#header.title}}
                         Bulbasaur
                       {{/header.title}}
                     {{/nacho-table-header}}`);

    assert.equal(findAll(`.${DefaultTableClasses.title}`).length, 2, 'Renders two title items');
    assert.equal(
      ((find(`.${DefaultTableClasses.title}:first-child`) as Element).textContent || '').trim(),
      'Charmander',
      'Renders the first column header correctly in the component blocks basic case'
    );
  });
});
