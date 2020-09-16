import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-table-title-cell';
import { classNames, tagName } from '@ember-decorators/component';
import { DefaultTableClasses } from '../constants/default-table-properties';

@tagName('th')
@classNames(DefaultTableClasses.title)
export default class NachoTableTitleCell extends Component {
  layout = layout;
}
