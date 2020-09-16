import { Factory, faker } from 'ember-cli-mirage';

/**
 * A list of static identifier fields to cycle through for testing purposes. This is to help reduce the variation
 * in identifier fields to a list of expected values
 */
const fieldNames = [
  'CONTACT_ID[type = long]',
  'DATA_XML_VERSION[type = long]',
  'DATA[type = string]',
  'DELETED_TS[type = long]',
  'GG_MODI_TS[type = long]',
  'GG_STATUS[type = string]',
  'IS_NOTE_MANUALLY_MOD[type = string]',
  'lumos_dropdate',
  'MODIFIED_DATE'
];

export default Factory.extend({
  comment: 'pocketmon',
  commentCount: null,
  dataType: 'DATATYPE',
  distributed: false,
  fieldName: faker.list.cycle(...fieldNames),
  fullFieldPath(): string {
    return this.fieldName as string;
  },
  id(id: number): number {
    return id;
  },
  indexed: false,
  nullable: false,
  parentSortID: 7,
  partitioned: false,
  sortId: 7,
  treeGridClass: null
});
