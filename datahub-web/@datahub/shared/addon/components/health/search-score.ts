import Component from '@glimmer/component';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import HealthProxy from '@datahub/shared/utils/health/health-proxy';
import { IEntityLinkAttrs } from '@datahub/data-models/types/entity/shared';

interface IHealthSearchScoreArgs {
  // the health score for the related search result item
  value: number;
  // The data model entity for which this score applies to
  entity?: DataModelEntityInstance;
}

const baseClass = 'search-health-score';

/**
 * Within the search context, the score is presented as a `value` attribute
 * This component adapts the value attribute to a score attribute on HealthScoreValue
 * Additionally will be used for tracking interactions from search context
 * @export
 * @class HealthSearchScore
 * @extends {Component<IHealthSearchScoreArgs>}
 */
export default class HealthSearchScore extends Component<IHealthSearchScoreArgs> {
  // Referenced from template
  baseClass = baseClass;

  /**
   * Class instance for the related health metadata with associated behavior
   * @readonly
   */
  get health(): HealthProxy {
    return new HealthProxy({ score: this.args.value, validations: [] });
  }

  /**
   * Generates a link object which references the Health Tab params for a DynamicLink component
   * @readonly
   */
  get linkParamsToEntityHealthTab(): IEntityLinkAttrs['link'] | void {
    const linkToHealthTab = this.args.entity?.entityTabLink('health');

    if (linkToHealthTab) {
      return linkToHealthTab.link;
    }
  }
}
