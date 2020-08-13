import Route from '@ember/routing/route';
import { IEntityTypeRouteModel } from '@datahub/shared/routes/entity-type';
export type EntityTypeUrnModel = IEntityTypeRouteModel & { urn: string };

/**
 * This route will capture the urn from the url and pass it down to the tab route
 **/
export default class EntityTypeUrn extends Route {
  /**
   * Will grab urn from url and pass it down to the next route
   * @param urn urn for the entity
   */
  model({ urn }: { urn: string }): EntityTypeUrnModel | void {
    const parentModel = this.modelFor('entity-type') as IEntityTypeRouteModel;
    if (parentModel) {
      return {
        ...parentModel,
        urn
      };
    }
  }
}
