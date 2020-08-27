import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { DataModelEntity, DataModelName } from '@datahub/data-models/constants/entity';
import { capitalize } from '@ember/string';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';

const queryParamsKeys: Array<string> = ['page', 'facets', 'path'];

/**
 * Arguments for the browse/search route.
 */
interface IBrowseSearchEntityParams {
  entity: DataModelName;
  path: string;
  facets: string;
  page: number;
}

/**
 * Route model return type. It will contain enough fields to perform a search query
 */
interface IBrowseSearchEntityModel {
  searchConfig: IEntityRenderCommonPropsSearch;
  trackPrefix: string;
  title: string;
  keyword: string;
  facets: string;
  page: number;
  entity: string;
}

/**
 * Search page for a browse path
 */
export default class BrowseSearchEntity extends Route.extend(AuthenticatedRouteMixin) {
  queryParams = refreshModelForQueryParams(queryParamsKeys);

  model(params: IBrowseSearchEntityParams): IBrowseSearchEntityModel {
    const { entity, path, facets, page } = params;
    const entityDataModel: DataModelEntity = DataModelEntity[entity];
    const { displayName, renderProps } = entityDataModel;
    const segments = path.split('/');
    return {
      searchConfig: renderProps.search,
      trackPrefix: `${displayName}path${path}`,
      title: `${capitalize(entity)} > ${segments.join(' > ')}`,
      keyword: `${entityDataModel.getQueryForHierarchySegments(segments)}`,
      facets,
      page,
      entity
    };
  }
}
