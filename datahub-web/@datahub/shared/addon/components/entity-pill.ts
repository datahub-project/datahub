import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/entity-pill';
import { layout } from '@ember-decorators/component';

@layout(template)
export default class EntityPill extends Component {
  baseClass: string = 'entity-pill';
}
