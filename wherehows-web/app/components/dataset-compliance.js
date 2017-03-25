import Ember from 'ember';

const {
  Component,
  computed,
  set,
  get,
  getWithDefault
} = Ember;

const complianceListKey = 'privacyCompliancePolicy.compliancePurgeEntities';
// List of field formats for security compliance
const fieldFormats = [
  'MEMBER_ID', 'SUBJECT_MEMBER_ID', 'URN', 'SUBJECT_URN', 'COMPANY_ID', 'GROUP_ID', 'CUSTOMER_ID'
];

/**
 * Computed macro tha checks if a dependentList has a specified privacy type
 * @param {String} dependentListKey
 * @param {String} type
 * @returns {boolean}
 */
const datasetHasPrivacyIdentifierType = (dependentListKey, type) => {
  const prop = 'identifierType';

  return computed(`${dependentListKey}.@each.${prop}`, {
    get() {
      const list = get(this, dependentListKey) || [];
      return list.filterBy(prop, type).length > 0;
    }
  });
};

export default Component.extend({
  sortColumnWithName: 'name',
  filterBy: 'name',
  sortDirection: 'asc',
  searchTerm: '',

  radioSelection: {
    memberId: null,
    subjectMemberId: null,
    urnId: null,
    orgId: null
  },

  hasMemberId: datasetHasPrivacyIdentifierType(complianceListKey, 'MEMBER_ID'),
  hasSubjectMemberId: datasetHasPrivacyIdentifierType(complianceListKey, 'SUBJECT_MEMBER_ID'),
  hasUrnId: datasetHasPrivacyIdentifierType(complianceListKey, 'URN'),
  hasOrgId: datasetHasPrivacyIdentifierType(complianceListKey, 'COMPANY_ID'),

  isMemberIdSelected: computed.notEmpty('radioSelection.memberId'),
  isMemberIdSelectedTrue: computed.equal('radioSelection.memberId', true),
  isSubjectMemberIdSelected: computed.notEmpty('radioSelection.subjectMemberId'),
  isUrnIdSelected: computed.notEmpty('radioSelection.urnId'),

  showSubjectMemberIdPrompt: computed.equal('radioSelection.memberId', false),
  showUrnIdPrompt: computed.or('isSubjectMemberIdSelected', 'isMemberIdSelectedTrue'),
  showOrgIdPrompt: computed.bool('isUrnIdSelected'),

  identifierTypes: ['', ...fieldFormats].map(type => ({
    value: type,
    label: type ? type.replace(/_/g, ' ').toLowerCase().capitalize() : 'Please select'
  })),

  complianceEntities: computed(`${complianceListKey}.[]`, function () {
    return getWithDefault(this, complianceListKey, []);
  }),

  ownerFieldIdType(field) {
    const ownerField = getWithDefault(this, complianceListKey, []).filterBy('identifierField', field).shift();
    return ownerField && ownerField.identifierType;
  },

  complianceFields: computed('complianceEntities', 'datasetSchemaFieldsAndTypes', function () {
    const complianceFieldNames = get(this, 'complianceEntities').mapBy('identifierField');

    return get(this, 'datasetSchemaFieldsAndTypes').map(({name, type}) => ({
      name,
      type,
      hasPrivacyData: complianceFieldNames.includes(name),
      format: get(this, 'ownerFieldIdType').call(this, name)
    }));
  }),

  changeFieldFormat(fieldName, format) {
    let field = get(this, 'complianceEntities').findBy('identifierField', fieldName);

    if (field && fieldFormats.includes(format)) {
      return set(field, 'identifierType', format);
    }
  },

  toggleFieldOnComplianceList(identifierField, toggle) {
    const complianceList = get(this, 'complianceEntities');
    const op = {
      add() {
        if (!complianceList.findBy('identifierField', identifierField)) {
          return complianceList.setObjects([...complianceList, {identifierField}]);
        }
      },

      remove: () => complianceList.setObjects(complianceList.filter(item => item.identifierField !== identifierField)),
    }[toggle];

    return typeof op === 'function' && op();
  },

  ensureTypeContainsFormat: (updatedCompliance) =>
    updatedCompliance.every(entity => fieldFormats.includes(get(entity, 'identifierType'))),

  actions: {
    onFieldFormatChange({name: fieldName}, {value: format}) {
      return this.changeFieldFormat(fieldName, format);
    },

    onFieldPrivacyChange({name: fieldName, hasPrivacyData}) {
      const toggle = !hasPrivacyData ? 'add' : 'remove';

      return this.toggleFieldOnComplianceList(fieldName, toggle);
    },

    saveCompliance () {
      const allEntitiesHaveValidFormat = this.ensureTypeContainsFormat(get(this, 'complianceEntities'));

      if (allEntitiesHaveValidFormat) {
        return this.get('onSave')();
      }
    },

    // Rolls back changes made to the compliance spec to current
    // server state
    resetCompliance () {
      this.get('onReset')();
    },

    didChangePrivacyIdentifiable (sectionName, isPrivacyIdentifiable) {
      const section = {
        'has-subject-member': 'subjectMemberId',
        'has-urn': 'urnId',
        'has-organization': 'orgId',
        'has-member': 'memberId'
      }[sectionName];

      return set(this, `radioSelection.${section}`, isPrivacyIdentifiable);
    }
  }
});
