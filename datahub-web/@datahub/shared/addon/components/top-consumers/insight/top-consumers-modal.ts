import Component from '@glimmer/component';
import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { IEntityListProfile } from '@datahub/shared/types/profile-list';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { IGridGroupEntity } from '@datahub/shared/types/grid-group';
import { TopConsumer } from '@datahub/metadata-types/constants/metadata/top-consumers';
import { IDynamicLinkParams } from 'dynamic-link/components/dynamic-link';
import { noop } from 'lodash';

interface ITopConsumersInsightTopConsumerModalArgs {
  /**
   * A list of instances of the top users
   */
  topUsers: Array<PersonEntity>;

  /**
   * A list of instances of the top groups
   */
  topGroups: Array<IGridGroupEntity>;

  /**
   * A list of the top user consumer urns
   */
  topUserUrns: Array<string>;

  /**
   * A map of link params that opens a new tab to the top group's entity page
   */
  topGroupLinkParams: Array<IDynamicLinkParams>;

  /**
   * The tab id currently selected in the top consumers modal
   */
  tabSelected: TopConsumer;

  /**
   * Action triggered when closing the top consumers modal
   */
  onCloseTopConsumersModal?: () => void;

  /**
   * Action triggered when selecting a different tab in the modal
   */
  tabSelectionDidChange?: () => void;
}

/**
 * The modal component for top consumers insight. It renders a tab for each top consumer type if available.
 */
export default class TopConsumersInsightTopConsumerModal extends Component<ITopConsumersInsightTopConsumerModalArgs> {
  /**
   * A list of tab properties consumed by the top consumers modal
   */
  get tabs(): Array<ITabProperties> {
    const numberOfTopUsers = this.args.topUserUrns.length;
    const numberOfTopGroups = this.args.topGroupLinkParams.length;
    const tabs = [];

    if (numberOfTopUsers) {
      tabs.push({
        id: TopConsumer.USER,
        title: `Individuals (${numberOfTopUsers})`,
        contentComponent: 'entity/people/profile-list'
      });
    }

    if (numberOfTopGroups) {
      tabs.push({
        id: TopConsumer.GROUP,
        title: `User Groups (${numberOfTopGroups})`,
        contentComponent: 'entity/grid-group/profile-list'
      });
    }

    return tabs;
  }

  get topUserProfiles(): Array<IEntityListProfile> {
    return this.args.topUserUrns.map(
      (_urn: string, index: number): IEntityListProfile => {
        return {
          entity: this.args.topUsers[index]
        };
      }
    );
  }

  get topGroupProfiles(): Array<IEntityListProfile> {
    return this.args.topGroupLinkParams.map(
      (linkParam: IDynamicLinkParams, index: number): IEntityListProfile => {
        return {
          entity: this.args.topGroups[index],
          linkParam: { ...linkParam, title: 'Hadoop Self Service' }
        };
      }
    );
  }

  /**
   * The current tab selected in the top consumers modal used to render the appropriate content for each tab in the modal
   */
  get topConsumerProfilesSelected(): Array<IEntityListProfile> {
    switch (this.args.tabSelected) {
      case TopConsumer.USER:
        return this.topUserProfiles;
      case TopConsumer.GROUP:
        return this.topGroupProfiles;
    }
  }

  /**
   * Action triggered when closing the modal
   */
  onCloseTopConsumersModal: () => void = this.args.onCloseTopConsumersModal || noop;

  /**
   * Action triggered when selecting a different tab in the modal
   */
  tabSelectionDidChange: () => void = this.args.tabSelectionDidChange || noop;
}
