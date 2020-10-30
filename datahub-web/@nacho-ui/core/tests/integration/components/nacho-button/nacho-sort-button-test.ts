import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { buttonClass, SortDirection, cycleSorting } from '@nacho-ui/core/components/nacho-button/nacho-sort-button';
import { computed, action } from '@ember/object';
import { tracked } from '@glimmer/tracking';

interface IPokemon {
  name: string;
  type: string;
  nature: string;
}

export const getTestSortObjects = (): Array<IPokemon> => [
  {
    name: 'pikachu',
    type: 'electric',
    nature: 'hardy'
  },
  {
    name: 'bulbasaur',
    type: 'grass',
    nature: 'bold'
  },
  {
    name: 'eevee',
    type: 'normal',
    nature: 'timid'
  }
];

export default class TestSortButton {
  @tracked
  isSorting = false;

  @tracked
  sortDirection = SortDirection.ASCEND;

  @tracked
  sortTestObjects = getTestSortObjects();

  @computed('sortTestObjects', 'isSorting', 'sortDirection')
  get pokemons(): Array<IPokemon> {
    const data = this.sortTestObjects.slice();

    if (this.isSorting) {
      data.sort((pokemonA, pokemonB) => {
        if (this.sortDirection === SortDirection.ASCEND) {
          return pokemonA.name > pokemonB.name ? 1 : -1;
        } else {
          return pokemonB.name > pokemonA.name ? 1 : -1;
        }
      });
    }

    return data;
  }

  @action
  onChangeSortProperty(): void {
    cycleSorting(this, 'isSorting', 'sortDirection');
  }
}

module('Integration | Component | nacho-sort-button', function(hooks) {
  setupRenderingTest(hooks);

  const baseClass = `.${buttonClass}`;
  const sortDefaultIconClass = '.fa-sort';
  const sortUpIconClass = '.fa-sort-up';
  const sortDownIconClass = '.fa-sort-down';

  test('it renders and behaves as expected', async function(assert) {
    await render(hbs`<NachoButton::NachoSortButton />`);
    assert.ok(this.element, 'Initial render is without errors');

    const testcontext = new TestSortButton();
    this.set('testcontext', testcontext);

    await render(hbs`<NachoButton::NachoSortButton
      @isSorting={{this.testcontext.isSorting}}
      @sortDirection={{this.testcontext.sortDirection}}
      @sortValue="pokemon"
      @baseClass="test-sort-button"
      @onChange={{fn this.testcontext.onChangeSortProperty}}
    />`);

    assert.ok(this.element, 'Initial render is without errors');
    assert.equal(findAll(baseClass).length, 1, 'A nacho sort button was rendered');

    // Testing sort icon button render
    assert.equal(findAll(sortDefaultIconClass).length, 1, 'Default sort rendered');
    assert.equal(findAll(sortDownIconClass).length, 0, 'Sanity check for sort icon');

    await click(baseClass);
    assert.equal(findAll(sortUpIconClass).length, 1, 'Rendering sort ascending');

    await click(baseClass);
    assert.equal(findAll(sortDownIconClass).length, 1, 'Rendering sort descending');
  });
});
