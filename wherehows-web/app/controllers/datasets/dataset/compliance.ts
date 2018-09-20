import Controller from '@ember/controller';
import { TagFilter } from 'wherehows-web/constants';

export default class DatasetComplianceController extends Controller {
  queryParams = [
    {
      fieldFilter: {
        as: 'field_filter'
      }
    }
  ];

  /**
   * Binding for field_filter query parameter used to set the filter value for the compliance tags
   * @type {(TagFilter | undefined)}
   * @memberof DatasetComplianceController
   */
  fieldFilter: TagFilter | undefined;
}
