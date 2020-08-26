import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-table-global';
import { classNames } from '@ember-decorators/component';
import { DefaultTableClasses } from '../constants/default-table-properties';

@classNames(DefaultTableClasses.global)
export default class NachoTableGlobal extends Component {
  layout = layout;
}
