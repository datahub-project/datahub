import Controller from '@ember/controller';
import { IBrowserRouteParams } from 'wherehows-web/routes/browse/entity';

/**
 * Handles query params for browse.entity route
 */
export default class extends Controller implements IBrowserRouteParams {
  queryParams = ['page', 'prefix', 'size', 'platform'];

  entity: IBrowserRouteParams['entity'];

  page = 1;

  prefix = '';

  platform = '';

  size = 10;
}
