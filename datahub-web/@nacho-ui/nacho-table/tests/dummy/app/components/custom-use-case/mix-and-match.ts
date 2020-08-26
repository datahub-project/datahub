import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../templates/components/custom-use-case/mix-and-match';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';

export default class CustomUseCaseMixAndMatch extends Component {
  layout = layout;

  basicData = [
    { name: 'Bulbasaur', type: 'Grass', nature: 'Hardy' },
    { name: 'Charmander', type: 'Fire', nature: 'Timid' },
    { name: 'Squirtle', type: 'Water', nature: 'Impish' }
  ];

  testConfigs: INachoTableConfigs<unknown> = {
    headers: [{ title: 'Names' }, { title: 'Types' }, { title: 'Natures' }],
    labels: ['name', 'type', 'nature'],
    useBlocks: { body: true, footer: true }
  };
}
