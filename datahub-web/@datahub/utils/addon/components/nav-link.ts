import LinkComponent from '@ember/routing/link-component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/nav-link';
// import { computed } from '@ember/object';
import { tagName, layout } from '@ember-decorators/component';

@tagName('li')
@layout(template)
export default class NavLink extends LinkComponent {
  attributeBindings = ['data-toggle', 'data-target'];
}
