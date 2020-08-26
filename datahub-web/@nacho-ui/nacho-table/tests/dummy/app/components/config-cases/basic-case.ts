import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../templates/components/config-cases/basic-case';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';

export default class ConfigCasesBasicCase<T> extends Component {
  layout = layout;

  basicData = [
    { name: 'Bulbasaur', type: 'Grass', nature: 'Hardy' },
    { name: 'Charmander', type: 'Fire', nature: 'Timid' },
    { name: 'Squirtle', type: 'Water', nature: 'Impish' }
  ];

  testConfigs: INachoTableConfigs<T> = {
    headers: [{ title: 'Names', className: 'pokemon-name' }, { title: 'Types' }, { title: 'Natures' }],
    labels: ['name', 'type', 'nature'],
    customColumns: {
      name: {
        className: 'custom-name-class'
      }
    }
  };
}
