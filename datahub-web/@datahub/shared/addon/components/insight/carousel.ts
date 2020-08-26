import Component from '@glimmer/component';
import { assert } from '@ember/debug';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { IInsightCarouselCardProps } from '@datahub/shared/types/insight/carousel/card';

interface IInsightCarouselArgs {
  // The entity instance for which an insight is being shown
  entity?: DataModelEntityInstance;
  // Options provided to the carousel which should include the cards / components to be rendered in the carousel
  // These options are typically provided by the entity render props configuration
  options?: {
    // List of insight components conforming to the IInsightCarouselCardProps interface to rendered
    components?: Array<IInsightCarouselCardProps>;
  };
}

/**
 * Renders a set of entity insight components supplied via the arguments to the component
 * Also renders these components sorted by a priority indicator
 * @export
 * @class InsightCarousel
 * @extends {Component<IInsightCarouselArgs>}
 */
export default class InsightCarousel extends Component<IInsightCarouselArgs> {
  /**
   * Ensures that each component to be rendered has a unique priority number
   * @private
   * @param {Array<IInsightCarouselCardProps>} insights properties for each component to be rendered
   */
  private checkInsightPrioritization(insights: Array<IInsightCarouselCardProps>): Array<IInsightCarouselCardProps> {
    const priorities = insights.map(({ options: { priority } = {} }): number | void => priority).filter(Boolean);
    const hasUniquePrioritization = new Set(priorities).size === priorities.length;
    assert('Expected each component with a defined priority to have a unique number', hasUniquePrioritization);

    return insights;
  }

  /**
   * Sorts the cards according to prioritization order in the carousel
   * @readonly
   */
  get sortedInsights(): Array<IInsightCarouselCardProps> {
    const insights: Array<IInsightCarouselCardProps> = this.args.options?.components || [];

    return this.checkInsightPrioritization(insights).sort(
      ({ options: { priority: priorityA } = {} }, { options: { priority: priorityB } = {} }) =>
        // Sort in ascending numerals if pA and pB are truthy and non-zero
        priorityA && priorityB ? priorityA - priorityB : 0
    );
  }
}
