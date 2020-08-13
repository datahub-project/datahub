import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../templates/components/config-cases/display-links';
import { INachoTableConfigs, NachoTableComputedLink } from '@nacho-ui/table/types/nacho-table';
import NachoTable from '@nacho-ui/table/components/nacho-table';

interface IBasicData {
  name: string;
  type: string;
  nature: string;
}

export default class ConfigCasesDisplayLinks extends Component {
  layout = layout;

  basicData: NachoTable<IBasicData>['data'] = [
    { name: 'Bulbasaur', type: 'Grass', nature: 'Hardy' },
    { name: 'Charmander', type: 'Fire', nature: 'Timid' },
    { name: 'Squirtle', type: 'Water', nature: 'Impish' }
  ];

  testConfigs: INachoTableConfigs<IBasicData> = {
    headers: [{ title: 'Names' }, { title: 'Types' }, { title: 'Natures' }, { title: 'Trainer' }],
    labels: ['name', 'type', 'nature', 'trainer'],
    customColumns: {
      name: {
        displayLink: {
          className: 'test-table-display-link',
          isNewTab: true,
          compute(rowData): NachoTableComputedLink {
            return { ref: `https://bulbapedia.bulbagarden.net/wiki/${rowData.name}_(Pokemon)`, display: rowData.name };
          }
        }
      }
    }
  };
}
