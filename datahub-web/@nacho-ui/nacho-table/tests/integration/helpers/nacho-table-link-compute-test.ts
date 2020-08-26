import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { NachoTableComputedLink } from '@nacho-ui/table/types/nacho-table';

module('Integration | Helper | nacho-table-link-compute', function(hooks) {
  setupRenderingTest(hooks);

  test('it computes the proper link', async function(assert) {
    const testRowData = { name: 'Pikachu', type: 'electric', role: 'detective' };
    const compute = function(rowData: typeof testRowData): NachoTableComputedLink {
      return {
        ref: `https://bulbapedia.bulbagarden.net/wiki/${rowData.name}_(Pok%C3%A9mon)`,
        display: rowData.name
      };
    };

    this.setProperties({
      compute,
      rowData: testRowData
    });

    await render(hbs`{{#let (nacho-table-link-compute compute rowData) as |testLink|}}
                       <a href={{testLink.ref}}>{{testLink.display}}</a>
                     {{/let}}`);
    assert.ok(this.element, 'Ended without errors');
    assert.equal((this.element.textContent || '').trim(), testRowData.name, 'Renders correct display');
    assert.equal(findAll('a').length, 1, 'Renders a link for the item');
    assert.equal(
      (find('a') as HTMLLinkElement).href,
      'https://bulbapedia.bulbagarden.net/wiki/Pikachu_(Pok%C3%A9mon)',
      'Computes the correct link based on input'
    );
  });
});
