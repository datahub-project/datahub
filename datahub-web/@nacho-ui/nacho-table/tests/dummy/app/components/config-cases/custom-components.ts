import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../templates/components/config-cases/custom-components';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';

export default class ConfigCasesCustomComponents<T> extends Component {
  layout = layout;

  basicData = [
    { name: 'Bulbasaur', type: 'Grass', nature: 'Hardy' },
    { name: 'Charmander', type: 'Fire', nature: 'Timid' },
    { name: 'Squirtle', type: 'Water', nature: 'Impish' }
  ];

  testConfigs: INachoTableConfigs<T> = {
    headers: [{ title: 'Names' }, { title: 'Types' }, { title: 'Natures' }, { title: 'Trainer' }],
    labels: ['name', 'type', 'nature', 'trainer'],
    customRows: { component: 'config-cases/helpers/test-row' }
  };
}
