// TODO: META-8742 Add this as a component in the Nacho Component library as part of NachoHoverDropdown
import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/nacho-hover-dropdown/dropdown-content';
import { layout } from '@ember-decorators/component';

@layout(template)
export default class NachoHoverDropdownDropdownContent extends Component {}
