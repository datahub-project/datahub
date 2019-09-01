import LinkComponent from '@ember/routing/link-component';
// import { computed } from '@ember/object';
import { tagName } from '@ember-decorators/component';

@tagName('li')
export default class NavLink extends LinkComponent {
  attributeBindings = ['data-toggle', 'data-target'];

  // params: Array<string>;
  // models: Array<string>;
  // route: string;

  // @computed('models', 'qualifiedRouteName')
  // get hrefForA(): string {
  //   const { params, models, route } = this;
  //   debugger;
  //   console.log(params, models);

  //   // if (this.get('loading')) {
  //   //   return this.get('loadingHref');
  //   // }
  //   this.hre

  //   let routing = this.get('_routing');
  //   let queryParams = this.get('queryParams.values');
  //   return href;
  // }
}
