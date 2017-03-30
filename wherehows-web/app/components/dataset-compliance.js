import Ember from 'ember';

const {
  Component,
  computed,
  set,
  get,
  isBlank,
  getWithDefault
} = Ember;

const complianceListKey = 'privacyCompliancePolicy.compliancePurgeEntities';
// TODO: DSS-6671 Extract to constants module
const logicalTypes = ['ID', 'URN', 'REVERSED_URN', 'COMPOSITE_URN'];
/**
 * Duplicate check using every to short-circuit iteration
 * @param {Array} names = [] the list to check for dupes
 * @return {Boolean} true is unique, false otherwise
 */
const fieldNamesAreUnique = (names = []) =>
  names.every((name, index) => names.indexOf(name) === index);

/**
 * Returns a computed macro based on a provided type will return a list of
 * Compliance fields that are of that identifierType or have no type
 * @param {String} type string to match against identifierType
 */
const complianceEntitiesMatchingType = type => {
  return computed('complianceDataFields.[]', function() {
    const fieldRegex = new RegExp(`${type}`, 'i');

    return get(this, 'complianceDataFields').filter(({ identifierType }) => {
      return fieldRegex.test(identifierType) || isBlank(identifierType);
    });
  });
};

export default Component.extend({
  sortColumnWithName: 'identifierField',
  filterBy: 'identifierField',
  sortDirection: 'asc',
  searchTerm: '',

  /**
   * Map of radio Group state values
   * Each initially has an indeterminate state, as the user
   * progresses through the prompts
   * @type {Object.<Boolean, null>}
   */
  userIndicatesDatasetHas: {
    member: null,
    org: null,
    group: null
  },

  didReceiveAttrs() {
    this._super(...arguments);
    // Perform validation step on the received component attributes
    this.validateAttrs();
  },

  /**
   * Ensure that props received from on this component
   * are valid, otherwise flag
   */
  validateAttrs() {
    const fieldNames = getWithDefault(
      this, 'schemaFieldNamesMappedToDataTypes', []
    ).mapBy('fieldName');

    if (fieldNamesAreUnique(fieldNames.sort())) {
      return;
    }

    // Flag this component's data as problematic
    set(this, '_hasBadData', true);
  },

  // Component ui state transitions based on the userIndicatesDatasetHas map
  hasUserRespondedToMemberPrompt: computed.notEmpty(
    'userIndicatesDatasetHas.member'
  ),

  showOrgPrompt: computed.bool('hasUserRespondedToMemberPrompt'),
  hasUserRespondedToOrgPrompt: computed.notEmpty('userIndicatesDatasetHas.org'),
  showGroupPrompt: computed.bool('hasUserRespondedToOrgPrompt'),

  // Map logicalTypes to options consumable by ui
  logicalTypes: ['', ...logicalTypes].map(value => ({
    value,
    label: value ? value.replace('_', ' ').toLowerCase().capitalize() : 'Please Select'
  })),

  /**
   * Lists all dataset fields found in the `columns` performs an intersection
   * of fields with the currently persisted and/or updated
   * privacyCompliancePolicy.compliancePurgeEntities.
   * The returned list is a map of fields with current or default privacy properties
   */
  complianceDataFields: computed(
    `${complianceListKey}.@each.identifierType`,
    `${complianceListKey}.[]`,
    'schemaFieldNamesMappedToDataTypes',
    function() {
      const sourceEntities = getWithDefault(this, complianceListKey, []);
      const complianceFieldNames = sourceEntities.mapBy('identifierField');

      const getAttributeOnField = (attribute, fieldName) => {
        const sourceField = getWithDefault(this, complianceListKey, []).find(
          ({ identifierField }) => identifierField === fieldName
        );
        return sourceField ? sourceField[attribute] : null;
      };

      /**
       * Get value for a list of attributes
       * @param {Array} attributes list of attribute keys to pull from
       *   sourceField
       * @param {String} fieldName name of the field to lookup
       * @return {Array} list of attribute values
       */
      const getAttributesOnField = (attributes = [], fieldName) =>
        attributes.map((attr) => getAttributeOnField(attr, fieldName));

      // Set default or if already in policy, retrieve current values from
      //   privacyCompliancePolicy.compliancePurgeEntities
      return getWithDefault(this, 'schemaFieldNamesMappedToDataTypes', [])
        .map(({ fieldName: identifierField, dataType }) => {
          const hasPrivacyData = complianceFieldNames.includes(identifierField);
          const [
            identifierType,
            isSubject,
            logicalType
          ] = getAttributesOnField([
            'identifierType',
            'isSubject',
            'logicalType'
          ], identifierField);

          return {
            dataType,
            identifierField,
            identifierType,
            isSubject,
            logicalType,
            hasPrivacyData
          };
        });
    }
  ),

  // Compliance entities filtered for each identifierType
  memberComplianceEntities: complianceEntitiesMatchingType('member'),
  orgComplianceEntities: complianceEntitiesMatchingType('organization'),
  groupComplianceEntities: complianceEntitiesMatchingType('group'),

  /**
   * Changes the logicalType on a field.
   *   Ensures that the logicalType / format is applicable to the specified field
   * @param {String} fieldName the fieldName identifying the field to be updated
   * @param {String} format logicalType or format te field is in
   * @return {String| void}
   */
  changeFieldLogicalType(fieldName, format) {
    const sourceField = get(this, complianceListKey).findBy(
      'identifierField',
      fieldName
    );

    if (sourceField && logicalTypes.includes(format)) {
      return set(sourceField, 'logicalType', String(format).toUpperCase());
    }
  },

  /**
   * Adds or removes a field onto the
   *  privacyCompliancePolicy.compliancePurgeEntities list.
   * @param {Object} props initial props for the field to be added
   * @prop {String} props.identifierField
   * @prop {String} props.dataType
   * @param {String} identifierType the type of the field to toggle
   * @param {('add'|'remove')} toggle operation to perform, can either be
   *   add or remove
   * @return {Ember.Array|*}
   */
  toggleFieldOnComplianceList(props, identifierType, toggle) {
    const { identifierField, dataType } = props;
    const sourceEntities = get(this, complianceListKey);

    if (!['add', 'remove'].includes(toggle)) {
      throw new Error(`Unsupported toggle operation ${toggle}`);
    }

    return {
      add() {
        // Ensure that we don't currently have this field present on the
        //  privacyCompliancePolicy.compliancePurgeEntities list
        if (!sourceEntities.findBy('identifierField', identifierField)) {
          const addPurgeEntity = { identifierField, identifierType, dataType };

          return sourceEntities.setObjects([addPurgeEntity, ...sourceEntities]);
        }
      },

      remove() {
        // Remove the identifierType since we are removing it from the
        //   privacyCompliancePolicy.compliancePurgeEntities in case it
        //   is added back during the session
        set(props, 'identifierType', null);
        return sourceEntities.setObjects(
          sourceEntities.filter(
            item => item.identifierField !== identifierField
          )
        );
      }
    }[toggle]();
  },

  /**
   * Checks that each privacyCompliancePolicy.compliancePurgeEntities has
   *  a valid identifierType
   * @param {Ember.Array} sourceEntities compliancePurgeEntities
   * @return {Boolean} has or does not
   */
  ensureTypeContainsFormat: sourceEntities =>
    sourceEntities.every(entity =>
      ['MEMBER', 'ORGANIZATION', 'GROUP'].includes(
        get(entity, 'identifierType')
      )),

  /**
   * Checks that each privacyCompliancePolicy.compliancePurgeEntities has
   *  a valid logicalType
   * @param {Ember.Array}sourceEntities compliancePurgeEntities
   * @return {Boolean|*} Contains or does not
   */
  ensureTypeContainsLogicalType: sourceEntities => {
    const logicalTypesInUppercase = logicalTypes.map(type => type.toUpperCase());

    return sourceEntities.every(entity =>
      logicalTypesInUppercase.includes(get(entity, 'logicalType')));
  },

  actions: {
    /**
     *
     * @param {String} identifierField id for the field to update
     * @param {String} logicalType updated format to apply to the field
     * @return {*|String|void} logicalType or void
     */
    onFieldFormatChange({ identifierField }, { value: logicalType }) {
      return this.changeFieldLogicalType(identifierField, logicalType);
    },

    /**
     * Toggles a field on / off the compliance list
     * @param {String} identifierType the type of the field to be toggled on
     *   the privacyCompliancePolicy.compliancePurgeEntities list
     * @param {Object|Ember.Object} props containing the props to be added
     * @prop {Boolean} props.hasPrivacyData checked or not checked
     * @return {*}
     */
    onFieldPrivacyChange(identifierType, props) {
      // If checked, add, otherwise remove
      const { hasPrivacyData } = props;
      const toggle = !hasPrivacyData ? 'add' : 'remove';

      return this.toggleFieldOnComplianceList(props, identifierType, toggle);
    },

    /**
     * Toggles the isSubject property of a member identifiable field
     * @param {Object} props the props on the member field to update
     * @prop {Boolean} isSubject flag indicating this field as a subject owner
     *   when true
     * @prop {String} identifierField unique field to update isSubject property
     */
    onMemberFieldSubjectChange(props) {
      const { isSubject, identifierField: name } = props;

      // Ensure that a flag isSubject is present on the props
      if (props && 'isSubject' in props) {
        const sourceField = get(this, complianceListKey).find(
          ({ identifierField }) => identifierField === name
        );

        set(sourceField, 'isSubject', !isSubject);
      }
    },

    /**
     * Updates the state flags that transition the prompts from one to the next
     * @param {String} sectionName name of the section that was changed
     * @param {Boolean} isPrivacyIdentifiable flag indicating that a section has
     *   or does not have privacy identifier
     */
    didChangePrivacyIdentifiable(sectionName, isPrivacyIdentifiable) {
      const section = {
        'has-group': 'group',
        'has-org': 'org',
        'has-member': 'member'
      }[sectionName];

      return set(
        this,
        `userIndicatesDatasetHas.${section}`,
        isPrivacyIdentifiable
      );
    },

    /**
     * If all validity checks are passed, invoke onSave action on controller
     */
    saveCompliance() {
      const allEntitiesHaveValidFormat = this.ensureTypeContainsFormat(
        get(this, complianceListKey)
      );
      const allEntitiesHaveValidLogicalType = this.ensureTypeContainsLogicalType(
        get(this, complianceListKey)
      );

      if (allEntitiesHaveValidFormat && allEntitiesHaveValidLogicalType) {
        return this.get('onSave')();
      }
    },

    // Rolls back changes made to the compliance spec to current
    // server state
    resetCompliance() {
      this.get('onReset')();
    }
  }
});
