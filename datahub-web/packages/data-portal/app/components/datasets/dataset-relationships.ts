import Component from '@ember/component';
import Configurator from 'wherehows-web/services/configurator';

export default class DatasetRelationships extends Component {
  showLineageGraph: boolean = Configurator.getConfig('showLineageGraph', {
    useDefault: true,
    default: false
  });
}
