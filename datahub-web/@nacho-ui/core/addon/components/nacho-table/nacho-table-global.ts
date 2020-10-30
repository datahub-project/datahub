import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../templates/components/nacho-table/nacho-table-global';
import { classNames } from '@ember-decorators/component';
import { DefaultTableClasses } from '@nacho-ui/core/constants/nacho-table/default-table-properties';

@classNames(DefaultTableClasses.global)
export default class NachoTableGlobal extends Component {
  layout = layout;
}
