import Component from '@ember/component';
import ComputedProperty, { alias } from '@ember/object/computed';
import { get, getProperties, computed } from '@ember/object';
import { action } from 'ember-decorators/object';
import {
  IComplianceChangeSet,
  IdentifierFieldWithFieldChangeSetTuple
} from 'wherehows-web/typings/app/dataset-compliance';
import { complianceFieldTagFactory } from 'wherehows-web/constants';

export default class DatasetComplianceRollupRow extends Component.extend({
  tagName: ''
}) {
  /**
   * References the parent external action to add a tag to the list of change sets
   */
  onFieldTagAdded: (tag: IComplianceChangeSet) => IComplianceChangeSet;

  /**
   * References the parent external action to add a tag to the list of change sets
   */
  onFieldTagRemoved: (tag: IComplianceChangeSet) => IComplianceChangeSet;

  /**
   * Flag indicating if the row is expanded or collapsed
   * @type {boolean}
   * @memberof DatasetComplianceRollupRow
   */
  isRowExpanded: boolean;

  /**
   * References the compliance field tuple containing the field name and the field change set properties
   * @type {IdentifierFieldWithFieldChangeSetTuple}
   * @memberof DatasetComplianceRollupRow
   */
  field: IdentifierFieldWithFieldChangeSetTuple;

  constructor() {
    super(...arguments);
    const isDirty: boolean = !!get(this, 'isRowDirty');

    // if any tag is dirty, then expand the parent row on instantiation
    this.isRowExpanded || (this.isRowExpanded = isDirty);
  }

  /**
   * References the first item in the IdentifierFieldWithFieldChangeSetTuple tuple, which is the field name
   * @type {ComputedProperty<string>}
   * @memberof DatasetComplianceRollupRow
   */
  identifierField: ComputedProperty<string> = alias('field.firstObject');

  /**
   * References the second item in the IdentifierFieldWithFieldChangeSetTuple type, this is the list of tags
   * for this field
   * @type {ComputedProperty<Array<IComplianceChangeSet>>}
   * @memberof DatasetComplianceRollupRow
   */
  fieldChangeSet: ComputedProperty<Array<IComplianceChangeSet>> = alias('field.1');

  /**
   * Aliases the dataType property on the first item in the field change set, this should available
   * regardless of if the field already exists on the compliance policy or otherwise
   * @type {ComputedProperty<string>}
   * @memberof DatasetComplianceRollupRow
   */
  dataType: ComputedProperty<string> = alias('fieldChangeSet.firstObject.dataType');

  /**
   * Checks if any of the field tags for this row are dirty
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetComplianceRollupRow
   */
  isRowDirty: ComputedProperty<boolean> = computed('fieldChangeSet', function(
    this: DatasetComplianceRollupRow
  ): boolean {
    return get(this, 'fieldChangeSet').some(tag => tag.isDirty);
  });

  /**
   * Toggles the expansion / collapse of the row expansion flag
   * @memberof DatasetComplianceRollupRow
   */
  @action
  onToggleRowExpansion() {
    this.toggleProperty('isRowExpanded');
  }

  /**
   * Handles adding a field tag when the user indicates the action through the UI
   * @memberof DatasetComplianceRollupRow
   */
  @action
  onAddFieldTag(this: DatasetComplianceRollupRow) {
    const { identifierField, dataType, onFieldTagAdded } = getProperties(this, [
      'identifierField',
      'dataType',
      'onFieldTagAdded'
    ]);

    if (typeof onFieldTagAdded === 'function') {
      onFieldTagAdded(complianceFieldTagFactory({ identifierField, dataType }));
    }
  }

  /**
   * Handles the removal of a field tag from the list of change set items
   * @param {IComplianceChangeSet} tag
   * @memberof DatasetComplianceRollupRow
   */
  @action
  onRemoveFieldTag(this: DatasetComplianceRollupRow, tag: IComplianceChangeSet) {
    const onFieldTagRemoved = get(this, 'onFieldTagRemoved');
    //@ts-ignore dot notation access is ts limitation with ember object model
    const isSoleTag = get(this, 'fieldChangeSet.length') === 1;
    ``;

    if (typeof onFieldTagRemoved === 'function' && !isSoleTag) {
      onFieldTagRemoved(tag);
    }
  }
}
