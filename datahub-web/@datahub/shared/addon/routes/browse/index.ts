import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { inject as service } from '@ember/service';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';
import { capitalize } from '@ember/string';
import DataModelsService from '@datahub/data-models/services/data-models';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

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
  browseHeaderComponents: Array<IDynamicComponent>;
}

export default class BrowseIndexRoute extends Route.extend(AuthenticatedRouteMixin) {
  @service('data-models')
  dataModels!: DataModelsService;

  model(): IBrowseIndexModel {
    const { dataModels } = this;
    const unGuardedEntities = this.dataModels.guards.unGuardedEntities.map(
      (entity): DataModelEntity => dataModels.getModel(entity.displayName)
    );
    const browseHeaderComponents = (unGuardedEntities
      .map((entity): Array<IDynamicComponent> | undefined => entity.renderProps.browseHeaderComponents)
      .filter(Boolean) as Array<Array<IDynamicComponent>>).reduce((acc, val) => acc.concat(val), []); //flat
    const entities: IBrowseIndexModel['entities'] = unGuardedEntities
      .filter((entity): boolean => Boolean(entity.renderProps.browse)) // filter entities that does not support browse
      .map(
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
      entities,
      browseHeaderComponents
    };
  }
}
