import Component from '@glimmer/component';
import { action } from '@ember/object';
import HealthProxy from '@datahub/shared/utils/health/health-proxy';
import { noop } from 'lodash';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

interface IHealthInsightCardArgs {
  // Reference to the Health metadata for the related entity if available
  health?: Com.Linkedin.Common.Health | null;
  // Action to be invoked when a user interacts with the CTA
  onViewHealth?: () => void;
  // Action to recalculate the current health score
  onRecalculateHealthScore?: () => void;
  // Flag indicating that the user is viewing the health tab of an entity,
  // if true, we can prevent the insight from showing an option to navigate to the tab
  isViewingHealth?: boolean;
}

// Block class for Health Insight card
export const baseClass = 'health-insight-card';

/**
 * Represents the related Health Metadata as an insight card
 * @export
 * @class HealthInsightCard
 * @extends {Component<IHealthInsightCardArgs>}
 */
export default class HealthInsightCard extends Component<IHealthInsightCardArgs> {
  /**
   * Template accessible reference to baseClass
   */
  baseClass = baseClass;

  /**
   * Text tooltip or description for the health score
   */
  tooltip = HealthProxy.descriptiveLabel;

  /**
   * List the dropdown options for the insight card menu
   */
  menuOptions: Array<INachoDropdownOption<string>> = [
    {
      label: 'Recalculate Score',
      value: ''
    }
  ];

  /**
   * Instantiates a proxy class for transformed health metadata attributes
   * @readonly
   */
  get healthProxy(): HealthProxy {
    return new HealthProxy(this.args.health);
  }

  /**
   * Invokes the action callback when a user clicks the view health CTA button
   */
  @action
  onViewHealthClicked(): void {
    const { onViewHealth = noop } = this.args;
    onViewHealth();
  }

  /**
   * Invokes the function to recalculate the health score if present
   */
  @action
  onRecalculateHealthScore(): void {
    const { onRecalculateHealthScore = noop } = this.args;
    onRecalculateHealthScore();
  }
}
