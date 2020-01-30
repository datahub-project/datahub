import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/user/profile/entity-header';
import { layout, classNames } from '@ember-decorators/component';
import { action, set, computed } from '@ember/object';
import { filter } from '@ember/object/computed';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

export const baseHeaderClass = 'user-entity-header';
export const baseInfoEditorClass = 'user-info-editor';

/**
 * Refers to the number of tags show on the skills or teams preview for a user profile
 * If the number of skills or teams is over the limit, they can be viewed in the view all modal
 */
const TAGS_SHOWN_LIMIT = 5;

/**
 * Filters a list of tags, keeping the first tags up to @TAGS_SHOWN_LIMIT to use as a preview of the list
 * @param {string} _tag
 * @param {number} index
 */
const tagsShownLimitFilter = function(_tag: string, index: number): boolean {
  return index < TAGS_SHOWN_LIMIT;
};

/**
 * The purpose of the UserProfileEntityHeader component is to display the details of a person's
 * entity page as well as be the component that facilitates the edit of such profile page
 */
@layout(template)
@classNames(`${baseHeaderClass}__container`)
export default class UserProfileEntityHeader extends Component {
  /**
   * Adding as property on the component for easy access witin the template
   */
  baseHeaderClass: string = baseHeaderClass;

  /**
   * Adding as property on the component for easy access witin the template
   */
  baseInfoEditorClass: string = baseInfoEditorClass;

  /**
   * The person entity that gives context to this entity-header
   */
  entity?: PersonEntity;

  /**
   * A preview of the list of skills to be displayed initially without selecting view all
   */
  @filter('entity.skills', tagsShownLimitFilter)
  skillsPreview?: Array<string>;

  /**
   * A preview of the list of teams to be displayed initially without selecting view all
   */
  @filter('entity.teamTags', tagsShownLimitFilter)
  teamsPreview?: Array<string>;

  /**
   * Renders a view all button that shows all skills in a modal if there are more skills than @TAGS_SHOWN_LIMIT
   */
  @computed('entity.skills')
  get renderViewAllSkillsButton(): boolean {
    return this.entity && this.entity.skills.length > TAGS_SHOWN_LIMIT ? true : false;
  }

  /**
   * Renders a view all button that shows all teams in a modal if there are more teams than @TAGS_SHOWN_LIMIT
   */
  @computed('entity.teamTags')
  get renderViewAllTeamsButton(): boolean {
    return this.entity && this.entity.teamTags.length > TAGS_SHOWN_LIMIT ? true : false;
  }

  /**
   * Flag used to show view all modal for skills
   */
  isViewingAllSkills = false;

  /**
   * Flag used to show view all modal for teams
   */
  isViewingAllTeams = false;

  /**
   * Whether or not the user is in editing profile mode
   */
  isEditingProfile = false;

  /**
   * Function triggered by user action to close the view all modal for skills
   */
  @action
  onCloseViewAllSkillsModal(): void {
    set(this, 'isViewingAllSkills', false);
  }

  /**
   * Function triggered by user action to close the view all modal for teams
   */
  @action
  onCloseViewAllTeamsModal(): void {
    set(this, 'isViewingAllTeams', false);
  }

  /**
   * Function triggered by user action in some way to close the profile info editor
   */
  @action
  onCloseEditorModal(): void {
    set(this, 'isEditingProfile', false);
  }
}
