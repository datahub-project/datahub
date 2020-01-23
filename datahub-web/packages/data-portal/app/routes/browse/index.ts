import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { inject as service } from '@ember/service';
import Search from 'wherehows-web/services/search';
import { set } from '@ember/object';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { unGuardedEntities } from 'wherehows-web/utils/entity/flag-guard';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';
import { capitalize } from '@ember/string';
import { getConfig } from 'wherehows-web/services/configurator';

/**
 * Links for browse entity. It needs the name of the string, and also it will
 * reset the queryParams (that is why `path: null`)
 */
type IBrowseEntityLink = IDynamicLinkNode<[string], 'browse.entity', { path: null }>;

/**
 * model contains link for the home page cards for the different entity types
 * of the app
 */
interface IBrowseIndexModel {
  entities: Array<IBrowseEntityLink>;
}

export default class BrowseIndexRoute extends Route.extend(AuthenticatedRouteMixin) {
  /**
   * Inject search service to set current entity
   */
  @service
  search: Search;

  /**
   * Resetting entity in search service when we navigate outside
   * any specific entity type
   */
  beforeModel(): void {
    set(this.search, 'entity', undefined);
  }

  model(): IBrowseIndexModel {
    const entities: IBrowseIndexModel['entities'] = unGuardedEntities(getConfig).map(
      (entity: DataModelEntity): IBrowseEntityLink => ({
        title: capitalize(entity.displayName),
        text: '',
        route: 'browse.entity',
        model: [String(entity.displayName).toLowerCase()], // Normalize casing between BaseEntity and entityDefinition type string values
        queryParams: {
          path: null
        }
      })
    );

    return {
      entities
    };
  }
}
