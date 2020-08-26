import Route from '@ember/routing/route';

/**
 * Lineage graph route.
 *
 * It will receive a urn and put it in the model
 */
export default class LineageUrn extends Route {
  /**
   * It requires a urn as an path parameter and put that urn in the model
   * @param { urn: string }
   */
  model({ urn }: { urn: string }): { urn: string } {
    return { urn };
  }
}
