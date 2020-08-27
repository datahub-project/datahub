import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/social/social-action';
import { layout, classNames, tagName, classNameBindings } from '@ember-decorators/component';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { inject as service } from '@ember/service';
import CurrentUser from '@datahub/shared/services/current-user';
import { computed } from '@ember/object';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { oneWay } from '@ember/object/computed';
import { SocialAction } from '@datahub/data-models/constants/entity/person/social-actions';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';

export const baseSocialActionComponentClass = 'social-action-container';

/**
 * Narrows down type of SocialAction to just the relevant types for this container
 */
type SupportedActions = SocialAction.LIKE | SocialAction.FOLLOW;

/**
 * A map to the icon given the type of social action we are working with
 */
const mapSocialActionToIcon: Record<SupportedActions, string> = {
  [SocialAction.LIKE]: 'thumbs-up',
  [SocialAction.FOLLOW]: 'bell'
};

/**
 * The component will render a button for semantic purposes, and a user clicking on this button will
 * trigger an action that writes to the likes or follows, depending on the specified purpose, and
 * use the PersonEntity reference to write the new information back.
 *
 * @example
 * Sample usage:
 * ```
 * <Social::SocialAction
 *  @entity={{DataModelEntity}}
 *  @type="like"
 * />
 * ```
 */
@layout(template)
@classNames(baseSocialActionComponentClass, 'nacho-button nacho-button--tertiary')
@classNameBindings(`hideSocialActions:${baseSocialActionComponentClass}--hidden`)
@tagName('button')
export default class SocialActionComponent extends Component {
  /**
   * Injected service for the current user
   */
  @service('current-user')
  currentUser!: CurrentUser;

  /**
   * Injected service for the configurator
   */
  @service
  configurator!: IConfigurator;

  /**
   * Attaching to the component for convenient template access
   */
  baseClass = baseSocialActionComponentClass;

  /**
   * The data entity that gives context to this container (that the user can like or follow)
   */
  entity?: DataModelEntityInstance;

  /**
   * The type of social action that this container represents
   * @default 'like'
   */
  type: SupportedActions = SocialAction.LIKE;

  /**
   * Sometimes this component may be used in a config based environment where "type" cannot be
   * easily passed in, so instead we have an options that performs the same purpose as a wrapper
   * around our needed parameters
   */
  options?: { type: SocialActionComponent['type'] };

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
   * Computes the action type for this component based on given parameters
   * @default 'like'
   */
  @computed('type', 'options')
  get actionType(): SocialActionComponent['type'] {
    const { type, options } = this;
    return (options && options.type) || type;
  }

  /**
   * References the current user as a PersonEntity from the current-user service
   */
  @oneWay('currentUser.entity')
  currentUserEntity?: PersonEntity;

  /**
   * Given the current user's liked or followed entities, returns whether or not the current
   * context entity is something that the user has liked/followed before
   */
  @computed('currentUserEntity', 'entity.{likedByUrns,followedByUrns}')
  get isActive(): boolean {
    const { entity, currentUserEntity, actionType } = this;

    if (currentUserEntity && entity) {
      const socialActionsUrnsList = actionType === SocialAction.LIKE ? entity.likedByUrns : entity.followedByUrns;
      return socialActionsUrnsList.includes(currentUserEntity.urn);
    }

    return false;
  }

  /**
   * The icon to be used for the container based on the type parameter
   */
  @computed('actionType')
  get actionIconName(): string {
    return mapSocialActionToIcon[this.actionType];
  }

  /**
   * Action to be taken when the component has been clicked by the user. Should trigger a follow
   * or like action on the entity that provides the container's context
   */
  click(): void {
    const { actionType, currentUserEntity, entity, isActive, hideSocialActions } = this;

    if (hideSocialActions) {
      return;
    }

    if (entity && currentUserEntity && entity.allowedSocialActions[actionType]) {
      if (actionType === SocialAction.LIKE) {
        isActive ? entity.removeLike() : entity.addLike();
      } else if (actionType === SocialAction.FOLLOW) {
        isActive ? entity.removeFollow() : entity.addFollow();
      }
    }
  }
}
