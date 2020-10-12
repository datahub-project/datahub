import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/social/containers/social-metadata';
import { layout, classNames, classNameBindings } from '@ember-decorators/component';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { task } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { inject as service } from '@ember/service';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { computed } from '@ember/object';
import { IDynamicComponentsTooltipArgs } from '@datahub/shared/components/dynamic-components/tooltip';
import { IDynamicComponentsIconArgs } from '@datahub/shared/components/dynamic-components/icon';
import { IDynamicComponentArgs } from '@datahub/shared/types/dynamic-component';
import { IDynamicComponentsTextArgs } from '@datahub/shared/components/dynamic-components/text';
import { IDynamicComponentsWikiLinkArgs } from '@datahub/shared/components/dynamic-components/wiki-link';
import { hasAspect } from '@datahub/data-models/entity/utils/aspects';

export const baseSocialMetadataComponentClass = 'social-metadata-container';

/**
 * Render props for the tooltip in our component
 */
const tooltipTriggerComponent: IDynamicComponentArgs<IDynamicComponentsIconArgs> = {
  name: 'dynamic-components/icon',
  options: { icon: 'question-circle', prefix: 'far', className: 'dynamic-tooltip__inline-trigger-icon' }
};

/**
 * Render props for the tooltip text in our component
 */
const tooltipTextContentComponent: IDynamicComponentArgs<IDynamicComponentsTextArgs> = {
  name: 'dynamic-components/text',
  options: {
    text: 'Likes are an upvote for the usefulness of an entity while follows will subscribe to notifications'
  }
};

/**
 * Render props for the tooltip wiki link in our component
 */
const tooltipWikiLinkComponent: IDynamicComponentArgs<IDynamicComponentsWikiLinkArgs> = {
  name: 'dynamic-components/wiki-link',
  options: { wikiKey: 'socialActions', text: 'Learn more' }
};

@layout(template)
@classNames(baseSocialMetadataComponentClass)
@classNameBindings(`hideSocialActions:${baseSocialMetadataComponentClass}--hidden`)
@containerDataSource<SocialMetadataContainer>('getContainerDataTask', ['entity'])
export default class SocialMetadataContainer extends Component {
  /**
   * Render props for the social actions tooltip component(s)
   */
  socialActionsTooltipProps: IDynamicComponentArgs<IDynamicComponentsTooltipArgs> = {
    name: 'dynamic-components/tooltip',
    options: {
      triggerComponent: tooltipTriggerComponent,
      className: `${baseSocialMetadataComponentClass}__entry`,
      triggerOn: 'hover',
      contentComponents: [tooltipTextContentComponent, tooltipWikiLinkComponent]
    }
  };

  /**
   * Injects the config service
   */
  @service
  configurator!: IConfigurator;

  /**
   * Attaching to the component for convenient template access
   */
  baseClass = baseSocialMetadataComponentClass;

  /**
   * The data entity that gives context to this container (that the user can like or follow)
   */
  entity?: DataModelEntityInstance;

  /**
   * Based on the configurator service provided value, we determine whether or not to hide the social
   * actions brought about by this component
   * @default true
   */
  @computed('configurator.showSocialActions')
  get hideSocialActions(): boolean {
    const { configurator } = this;
    return !(configurator && configurator.getConfig('showSocialActions', { useDefault: true, default: false }));
  }

  /**
   * Gets the link for social actions from our wiki links configuration to provide more help to the
   * user
   */
  @computed('configurator.wikiLinks')
  get moreInfoLink(): string | undefined {
    const { configurator } = this;
    const wikiLinks = configurator.getConfig('wikiLinks');
    return wikiLinks.socialActions;
  }

  /**
   * Gets a flag whether to show upcoming social aspect features that are currently in progress
   * @default false
   */
  @computed('configurator.showPendingSocialActions')
  get showPendingSocialActions(): boolean {
    const { configurator } = this;
    return configurator && configurator.getConfig('showPendingSocialActions', { useDefault: true, default: false });
  }

  /**
   * Task defined to get the data necessary for this container to operate.
   */
  @(task(function*(this: SocialMetadataContainer): IterableIterator<Promise<Array<void>>> {
    const { entity, hideSocialActions } = this;
    // Prevents making unnecessary calls to the API if we are already flag guarding this feature anyway
    // or we already have the aspect loaded
    const load = [];

    if (!hideSocialActions && entity && !hasAspect(entity, 'likes')) {
      load.push(entity.readLikes());
    }

    if (!hideSocialActions && entity && !hasAspect(entity, 'follow')) {
      load.push(entity.readFollows());
    }

    yield Promise.all(load);
  }).drop())
  getContainerDataTask!: ETaskPromise<Array<void>>;
}
