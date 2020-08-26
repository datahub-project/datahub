import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-table-footer';
import { classNames, tagName } from '@ember-decorators/component';
import { DefaultTableClasses } from '../constants/default-table-properties';

@tagName('tfoot')
@classNames(DefaultTableClasses.footer)
export default class NachoTableFooter extends Component {
  layout = layout;
}
