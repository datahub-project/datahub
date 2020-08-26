import Controller from '@ember/controller';

/**
 * Defines the class for datasets/dataset/tab route controller
 * Provides access to application objects like the BannerService instance
 * to templates and components lower in the hierarchy
 * @export
 * @class DatasetTab
 * @extends {Controller}
 */
export default class DatasetTab extends Controller {
  /**
   * TODO META-10613: Create a util for extending controller/query param logic for bulk jit request
   * Defines the query parameters meaningful to this route
   */
  queryParams = ['request_jit_urns'];

  /**
   * A list of dataset urns to request for JIT ACL Access provided as a query param
   * The query param renders the request-access modal if there are urns provided
   * @type {Array<string>}
   */
  request_jit_urns: Array<string> = [];
}

declare module '@ember/controller' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'datasets/dataset/tab': DatasetTab;
  }
}
