import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/test-basic-dropdown';
import { NachoDropdownOptions } from '@nacho-ui/dropdown/types/nacho-dropdown';
import { set } from '@ember/object';

export default class TestBasicDropdown extends Component.extend({
  // anything which *must* be merged to prototype here
}) {
  layout = layout;

  sampleOptions: NachoDropdownOptions<string | undefined> = [
    { label: 'Selected a value!', isDisabled: true, value: undefined },
    { label: 'Pikachu', value: 'pikachu' },
    { label: 'Eevee', value: 'eevee' },
    { label: 'Charmander', value: 'charmander' }
  ];

  selected!: string;

  selectionChange(selected: string): void {
    set(this, 'selected', selected);
  }
}
