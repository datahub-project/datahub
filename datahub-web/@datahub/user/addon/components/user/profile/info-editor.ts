import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/user/profile/info-editor';
import { layout, classNames } from '@ember-decorators/component';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { computed, set, action, setProperties } from '@ember/object';
import { baseInfoEditorClass } from '@datahub/user/components/user/profile/entity-header';
import { noop } from 'lodash';
import { IPersonEntityEditableProperties } from '@datahub/data-models/types/entity/person/props';
import { alias } from '@ember/object/computed';

/**
 * The maximum character limit for the summary about me
 */
const MAX_CHAR_SUMMARY = 200;

/**
 * The maximum character limit for skill and team tag inputs
 */
const MAX_CHAR_TAG_INPUT = 48;

/**
 * The InfoEditor class is meant to allow the user to edit the editable properties of their own
 * PersonEntity
 */
@layout(template)
@classNames(baseInfoEditorClass)
export default class UserProfileInfoEditor extends Component {
  /**
   * Attaching to component for convenient access in the template
   */
  baseInfoEditorClass: string = baseInfoEditorClass;

  /**
   * Attaching to component for convenient access in the template
   */
  maxCharSummary: number = MAX_CHAR_SUMMARY;

  /**
   * Attaching to component for convenient access in the template
   */
  maxCharTagInput: number = MAX_CHAR_TAG_INPUT;

  /**
   * The person entity that gives context to this editor context
   */
  entity?: PersonEntity;

  /**
   * The hash of editable working properties for this editor component
   */
  editedProps: IPersonEntityEditableProperties = this.defaultPersonEditableProperties;

  /**
   * Whether or not we are in an await - saving state
   */
  isSaving = false;

  /**
   * External action used to trigger a parent component that the user has desired to close the
   * editor window
   */
  onCloseEditor: () => void = noop;

  /**
   * External action used to inform the data layer that the edited PersonEntity properties should
   * be persisted
   */
  onSave: (savedInfo: IPersonEntityEditableProperties) => Promise<void> = (
    _savedInfo: IPersonEntityEditableProperties
  ): Promise<void> => Promise.resolve();

  /**
   * Gives the default editable props, helpful for resetting the blank editable props after a
   * save or cancel function
   */
  get defaultPersonEditableProperties(): IPersonEntityEditableProperties {
    return {
      focusArea: '',
      skills: [],
      teamTags: []
    };
  }

  /**
   * The current character count of the summary text while editing user profile
   * Displayed for user awareness to stay under @MAX_CHAR_SUMMARY limit
   */
  @alias('editedProps.focusArea.length')
  summaryCharCount!: number;

  /**
   * Computes from either the original entity or the current edited values the working value of the
   * PersonEntity focus area
   */
  @computed('editedProps.focusArea')
  get focusArea(): PersonEntity['focusArea'] {
    return this.editedProps.focusArea;
  }

  set focusArea(value: string) {
    set(this, 'editedProps', { ...this.editedProps, focusArea: value || '' });
  }

  /**
   * Computes, from either the original entity or the current edited values, the working value of
   * the PesronEntity skills
   */
  @computed('editedProps.skills')
  get skills(): PersonEntity['skills'] {
    return this.editedProps.skills;
  }

  set skills(value: Array<string>) {
    set(this, 'editedProps', { ...this.editedProps, skills: value || [] });
  }

  /**
   * Computes, from either the original entity or the current edited values, the working value of
   * the PersonEntity team tags
   */
  @computed('editedProps.teamTags')
  get teamTags(): PersonEntity['teamTags'] {
    return this.editedProps.teamTags;
  }

  set teamTags(value: Array<string>) {
    set(this, 'editedProps', { ...this.editedProps, teamTags: value || [] });
  }

  /**
   * Takes advantage of the hook to populate initial data for our form from the entity, if we have
   * proper access to one.
   *
   * Note: This behavior was previously a computed property, however, we only want the values to be
   * computed from the entity once. If the person decides to change the values, they should be
   * independent from the entity at that point. A computed property had the unintended effect of
   * accidentally subsequently returning the entity values again and overriding the form
   */
  didInsertElement(): void {
    if (this.entity) {
      const { focusArea, teamTags, skills } = this.entity;
      set(this, 'editedProps', {
        focusArea,
        teamTags,
        skills
      });
    }
  }

  /**
   * Triggers the save process for the editor, awaiting the result of the save and then closing/
   * reseting the working information
   */
  @action
  async onSaveEditor(): Promise<void> {
    const { focusArea, skills, teamTags } = this;
    set(this, 'isSaving', true);
    await this.onSave({ focusArea, skills, teamTags });
    this.onResetEditor();
  }

  /**
   * Closes the editor and resets the working values to default
   */
  @action
  onResetEditor(): void {
    setProperties(this, {
      editedProps: this.defaultPersonEditableProperties,
      isSaving: false
    });
    this.onCloseEditor();
  }

  /**
   * Removes a tag from one of the lists of tags that we are working with in the editor
   * @param {string} key - the key of the list from which the tag is to be removed
   * @param {number} valueIndex - the value to remove from that list identified by index
   */
  @action
  removeTag(key: 'skills' | 'teamTags', valueIndex: number): void {
    set(
      this,
      key,
      this[key].filter((_tag, idx): boolean => idx !== valueIndex)
    );
  }

  /**
   * Adds a tag to one of the lists of tags that we are working with in the editor
   * @param {string} key - the key of the list to which the tag is to be added
   * @param {string} value - the value to add to the list
   */
  @action
  addTag(key: 'skills' | 'teamTags', value: string): void {
    set(this, key, this[key].concat(value));
  }
}
