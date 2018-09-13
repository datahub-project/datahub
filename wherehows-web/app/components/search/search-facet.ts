import Component from '@ember/component';
import { computed } from '@ember-decorators/object';

export default class SearchFacet extends Component {
  // tagName = '';

  selections: any;

  @computed('selections')
  get showClear(): boolean {
    const selections = this.selections || {};
    return Object.keys(selections).reduce((willShow: boolean, selectionKey: string) => {
      return willShow || selections[selectionKey];
    }, false);
  }
}
