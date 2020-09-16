import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../../templates/components/config-cases/helpers/test-row';
import { computed } from '@ember/object';
import { tagName } from '@ember-decorators/component';
import { capitalize } from '@ember/string';

@tagName('tr')
export default class ConfigCasesHelpersTestRow<T extends { name: string }> extends Component.extend({
  // anything which *must* be merged to prototype here
}) {
  layout = layout;
  // normal class body definition here

  rowData!: T;

  @computed('rowData')
  get renderedData(): T {
    const row = this.rowData || {};
    return {
      ...row,
      trainer: 'Ash Ketchum',
      newname: (row.name || '')[0] + capitalize(row.name || '').slice(1)
    };
  }
}
