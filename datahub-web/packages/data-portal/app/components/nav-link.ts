import LinkComponent from '@ember/routing/link-component';
// import { computed } from '@ember/object';
import { tagName } from '@ember-decorators/component';

@tagName('li')
export default class NavLink extends LinkComponent {
  attributeBindings = ['data-toggle', 'data-target'];
}
