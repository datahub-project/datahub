import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-table-cell';
import { classNames, tagName } from '@ember-decorators/component';
import { DefaultTableClasses } from '../constants/default-table-properties';

@tagName('td')
@classNames(DefaultTableClasses.cell)
export default class NachoTableCell extends Component {
  layout = layout;
}
