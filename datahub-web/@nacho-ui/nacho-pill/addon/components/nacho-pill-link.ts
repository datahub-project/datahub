import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-pill-link';
import { classNames, tagName, className } from '@ember-decorators/component';

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

@classNames('nacho-pill', 'nacho-pill--link')
@tagName('span')
export default class NachoPillLink extends Component {
  layout = layout;

  /**
   * Determination of whether this should be rendered as an "active" pill link, which will apply the relevant
   * styling theme
   * @type {boolean}
   */
  @className('nacho-pill--link--active') isActive!: boolean;

  /**
   * Determines the content to render inside the given link.
   * @type {string}
   */
  text!: string;

  /**
   * The route path to link to using the provided helper.
   * @type {string}
   */
  linkTo!: string;

  /**
   * Query parameters that can optionally be passed to our helper
   * @type {IKeyMap}
   */
  queryParams!: IKeyMap;

  /**
   * If the user has provided query parameters, then we have to do a little bit of Ember rigging to get
   * the {{link-to}} helper to behave how we want with query parameters
   * https://discuss.emberjs.com/t/passing-dynamic-query-params-as-an-object-in-link-to-helper/9136/3
   * @type {LinkToParams}
   */
  linkToParams!: LinkToParams;

  init(): void {
    super.init();

    const queryParams = this.queryParams;
    const linkToPaths: LinkToParams = (this.linkTo || '').split('.');
    this.linkToParams = queryParams
      ? linkToPaths.concat({
          isQueryParams: true,
          values: {
            ...queryParams
          }
        })
      : linkToPaths;
  }
}
