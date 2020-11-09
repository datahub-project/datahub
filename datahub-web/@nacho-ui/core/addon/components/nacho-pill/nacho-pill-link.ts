import Component from '@glimmer/component';
import { computed, action } from '@ember/object';

/**
 * Generic typing for an object whose keys can be any value. Should be replaced from nacho core typings once
 * available
 */
interface IKeyMap {
  [key: string]: unknown;
}

/**
 * Object read by Ember's {{link-to}} helper as a query parameters compoment. This should be moved to nacho-core
 */
interface IQueryParams {
  isQueryParams: boolean;
  values: IKeyMap;
}

/**
 * Defines the params that are passed into the {{link-to}} helper to determine route path and query params
 */
type LinkToParams = Array<string | IQueryParams>;

interface INachoPillLinkArgs {
  /**
   * Additional css classes
   */
  class?: string;
  /**
   * Determination of whether this should be rendered as an "active" pill link, which will apply the relevant
   * styling theme
   * @type {boolean}
   */
  isActive: boolean;

  /**
   * Determines the content to render inside the given link.
   * @type {string}
   */
  text: string;

  /**
   * The route path to link to using the provided helper.
   * @type {string}
   */
  linkTo: string;

  /**
   * Query parameters that can optionally be passed to our helper
   * @type {IKeyMap}
   */
  queryParams: IKeyMap;

  /**
   * Optional click event in case you need tracking
   */
  click: () => void;
}

export default class NachoPillLink extends Component<INachoPillLinkArgs> {
  /**
   * If the user has provided query parameters, then we have to do a little bit of Ember rigging to get
   * the {{link-to}} helper to behave how we want with query parameters
   * https://discuss.emberjs.com/t/passing-dynamic-query-params-as-an-object-in-link-to-helper/9136/3
   * @type {LinkToParams}
   */
  @computed('args.{queryParams,linkTo}')
  get linkToParams(): LinkToParams {
    const queryParams = this.args.queryParams;
    const linkToPaths: LinkToParams = (this.args.linkTo || '').split('.');
    return queryParams
      ? linkToPaths.concat({
          isQueryParams: true,
          values: {
            ...queryParams
          }
        })
      : linkToPaths;
  }

  /**
   * Internal handler for click
   */
  @action
  onClick(): void {
    this.args.click && this.args.click();
  }
}
