import Component from '@glimmer/component';
import { set, action, computed } from '@ember/object';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { TopConsumer } from '@datahub/metadata-types/constants/metadata/top-consumers';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { inject as service } from '@ember/service';

interface ITopConsumersInsightArgs {
  // The context entity that the top consumers insight is describing
  entity?: DataModelEntityInstance;
  // options for rendering the Top Consumers insight
  options?: {
    // The name of the component to render within the Top Consumers insight
    component: string;
    // Flag indicating that concurrency task handler component should only handle async loading
    isOptional: boolean;
  };
}

/**
 * The top consumers insight component is the presentational interface for top consumers.
 * It renders different insight components depending on the renderProps it was given.
 * This component also contains logic for rendering a modal that renders a full UI of all top consumer aspect data.
 */
export default class TopConsumersInsight extends Component<ITopConsumersInsightArgs> {
  /**
   * Injects the config service
   */
  @service
  configurator!: IConfigurator;

  /**
   * Top consumer enum declared in the component for convenient access
   */
  topConsumer = TopConsumer;

  /**
   * The tooltip displayed in the insight for top users
   */
  topUsersTooltipText = 'Individuals using this dataset most frequently';

  /**
   * The tooltip displayed in the insight for top groups
   */
  topGroupsTooltipText = 'Headless accounts (groups on Hadoop Grid) using this dataset most frequently';

  /**
   * Flag determining whether top consumers modal is being shown
   */
  isShowingTopConsumersModal = false;

  /**
   * The tab id currently selected in the top consumers modal
   */
  tabSelected?: TopConsumer;

  /**
   * Gets the flag guard for showing the top consumers insight
   * @default false
   */
  @computed('configurator.showTopConsumers')
  get showTopConsumers(): boolean {
    const { configurator } = this;
    return configurator && configurator.getConfig('showTopConsumers', { useDefault: true, default: false });
  }

  /**
   * Action triggered when the tab is changed in the top consumers modal
   * @param tab the tab that is being selected
   */
  @action
  tabSelectionDidChange(tab: TopConsumer): void {
    set(this, 'tabSelected', tab);
  }

  /**
   * Action triggered to show the top consumers modal
   * @param tab the tab that the top consumers modal will show on render
   */
  @action
  showTopConsumersModal(tab?: TopConsumer): void {
    if (tab) {
      this.tabSelectionDidChange(tab);
      set(this, 'isShowingTopConsumersModal', true);
    }
  }

  /**
   * Action triggered to close the top consumers modal
   */
  @action
  closeTopConsumersModal(): void {
    set(this, 'isShowingTopConsumersModal', false);
  }
}
