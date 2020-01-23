import { DataModelName } from '@datahub/data-models/constants/entity';

/**
 * Describes the route parameter interface for the browser route
 * @export
 * @interface IBrowserRouteParams
 */
export interface IBrowserRouteParams {
  entity: DataModelName;
  page: number;
  size: number;
  path: string;
}
