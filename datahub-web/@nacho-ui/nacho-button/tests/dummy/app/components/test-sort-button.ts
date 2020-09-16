import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/test-sort-button';
import { layout, classNames } from '@ember-decorators/component';
import { action, computed } from '@ember/object';
import { SortDirection, cycleSorting } from '@nacho-ui/button/components/nacho-sort-button';

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

@layout(template)
@classNames('test-sort-container')
export default class TestSortButton extends Component {
  isSorting = false;

  sortDirection = SortDirection.ASCEND;

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
