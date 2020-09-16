import Component from '@glimmer/component';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { IInsightCarouselCardProps } from '@datahub/shared/types/insight/carousel/card';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { inject as service } from '@ember/service';

interface IHealthCarouselInsightArgs {
  // options for this insight passed in from the Insight::Carousel
  options?: IInsightCarouselCardProps['options'];
  // The instance of the related DataModelEntity for which this insight is to be retrieved
  entity?: DataModelEntityInstance;
}

/**
 * Health Insight component for rendering within a carousel component wraps the insight metadata
 * components including the container in a portable self-contained component
 * @export
 * @class HealthCarouselInsight
 * @extends {Component<IHealthCarouselInsightArgs>}
 */
export default class HealthCarouselInsight extends Component<IHealthCarouselInsightArgs> {
  /**
   * Temporary reference to the configurator service used here for feature flag
   */
  @service
  configurator!: IConfigurator;

  /**
   * Guard for Health insight feature
   * @readonly
   */
  get isHealthInsightEnabled(): boolean {
    return this.configurator.getConfig('useVersion3Health', { useDefault: true, default: false });
  }
}
