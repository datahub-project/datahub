import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../../templates/components/entity-page/entity-header/content-row';
import { classNames } from '@ember-decorators/component';

@classNames('wherehows-entity-header-content-row')
export default class EntityHeaderContentRow extends Component {
  layout = layout;
}
