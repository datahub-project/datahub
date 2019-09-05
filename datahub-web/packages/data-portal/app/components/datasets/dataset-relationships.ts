import Component from '@ember/component';
import { getConfig } from 'wherehows-web/services/configurator';

export default class DatasetRelationships extends Component {
  showLineageGraph: boolean = getConfig('showLineageGraph', {
    useDefault: true,
    default: false
  });
}
