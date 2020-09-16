import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { IDatasetComplianceInfo } from '@datahub/metadata-types/types/entity/dataset/compliance/info';
import DatasetComplianceInfo, {
  removeReadonlyAttributeFromTags,
  getEditableTags,
  removeTagsMissingIdentifierAttributes
} from '@datahub/data-models/entity/dataset/modules/compliance-info';
import { isArray } from '@ember/array';
import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';
import { IComplianceFieldAnnotation } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-annotation';
import {
  ComplianceFieldIdValue,
  Classification
} from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';
import DatasetSchema from '@datahub/data-models/entity/dataset/modules/schema';

module('Unit | Utility | entity/dataset/modules/compliance-info', function(hooks): void {
  setupTest(hooks);

  const rawComplianceInfo: IDatasetComplianceInfo = {
    complianceEntities: [],
    compliancePurgeNote: null,
    complianceType: '',
    confidentiality: null,
    containingPersonalData: null,
    datasetClassification: null,
    datasetId: null,
    datasetUrn: 'pikachu_dataset_urn',
    modifiedBy: 'catran',
    modifiedTime: 1552443949,
    fromUpstream: false
  };

  const complianceEntities: Array<IComplianceFieldAnnotation> = [
    {
      identifierField: 'testFieldA',
      identifierType: ComplianceFieldIdValue.None,
      logicalType: null,
      nonOwner: true,
      pii: false,
      readonly: true,
      securityClassification: null
    },
    {
      identifierField: 'testFieldB',
      identifierType: ComplianceFieldIdValue.None,
      logicalType: null,
      nonOwner: true,
      pii: false,
      readonly: false,
      securityClassification: null
    }
  ];

  test('DatasetComplianceInfo class', function(assert): void {
    const complianceInfo = new DatasetComplianceInfo(rawComplianceInfo, []);

    assert.ok(
      isArray(complianceInfo.annotations),
      'Creates a proper list for annotations from complianceEntities property'
    );
    assert.equal(complianceInfo.annotations.length, 0, 'Sanity check: Compliance annotations are currently empty');

    complianceInfo.addAnnotation(new DatasetComplianceAnnotation());
    assert.equal(complianceInfo.annotations.length, 1, 'Adds a compliance annotation');

    complianceInfo.removeAnnotation(new DatasetComplianceAnnotation());
    assert.equal(complianceInfo.annotations.length, 0, 'Removes annotation with function');

    complianceInfo.addAnnotation([new DatasetComplianceAnnotation(), new DatasetComplianceAnnotation()]);
    assert.equal(complianceInfo.annotations.length, 2, 'Expected 2 annotations to be added to compliance');

    complianceInfo.removeAnnotation([new DatasetComplianceAnnotation(), new DatasetComplianceAnnotation()]);
    assert.equal(complianceInfo.annotations.length, 0, 'Expected 2 annotations to be removed to compliance');

    const complianceAnnotation: IComplianceFieldAnnotation = {
      identifierField: 'eevee',
      identifierType: ComplianceFieldIdValue.MemberId,
      nonOwner: true,
      pii: false,
      readonly: false,
      securityClassification: null
    };

    complianceInfo.addAnnotation(new DatasetComplianceAnnotation(complianceAnnotation));
    const workingAnnotations = complianceInfo.getWorkingAnnotations();
    const readWorkingCopy = workingAnnotations[0];

    assert.equal(
      readWorkingCopy.identifierField,
      complianceAnnotation.identifierField,
      'Working copy works as expected'
    );
    assert.equal(readWorkingCopy.identifierType, complianceAnnotation.identifierType, 'Working copy sanity check');
  });

  test('Getting list of added or removed annotations', function(assert): void {
    const complianceInfo = new DatasetComplianceInfo(rawComplianceInfo, []);
    const [testA, testB] = complianceEntities;
    const annotationsToBeAdded = [new DatasetComplianceAnnotation(testA), new DatasetComplianceAnnotation(testB)];
    complianceInfo.addAnnotation(annotationsToBeAdded);
    assert.equal(complianceInfo.annotations.length, 2, 'Expected 2 annotations to be added to compliance');
    const { added, removed } = complianceInfo.getAnnotationUpdates();

    assert.equal(removed.length, 0, 'Expected no annotations to be removed');
    assert.equal(added.length, complianceInfo.annotations.length, 'Expected the number of added annotations to match');

    assert.ok(
      annotationsToBeAdded.includes(added[0]),
      'Expected added annotations to include item from annotationsToBeAdded'
    );

    complianceInfo.removeAnnotation(annotationsToBeAdded[0]);
    const updatedAddedAnnotations = complianceInfo.getAnnotationUpdates().added;

    assert.equal(
      updatedAddedAnnotations.length,
      1,
      'Expected the list for annotations to be added to not contain the removed item'
    );
  });

  test('compliance info utility function: removeReadonlyAttributeFromTags', function(assert): void {
    const tagsWithoutReadonlyAttribute = removeReadonlyAttributeFromTags(complianceEntities);

    assert.equal(
      tagsWithoutReadonlyAttribute.filter((tag: Omit<IComplianceFieldAnnotation, 'readonly'>): boolean =>
        tag.hasOwnProperty('readonly')
      ).length,
      0,
      'Expected no tags to have a readonly attribute'
    );
  });

  test('compliance info utility function: getEditableTags', function(assert): void {
    const editableTags = getEditableTags(complianceEntities);

    assert.equal(editableTags.length, 1, 'Expected only one editable tag');
    assert.equal(
      editableTags[0].identifierField,
      'testFieldB',
      'Expected editable tag identifierField to be "testFieldB"'
    );
  });

  test('compliance info utility function: removeTagsMissingIdentifierAttributes', function(assert): void {
    const tagsWithOneMissingAnIdentifierAttribute = complianceEntities.map(
      (entity: IComplianceFieldAnnotation): IComplianceFieldAnnotation =>
        entity.identifierField === 'testFieldB' ? { ...entity, identifierType: undefined } : entity
    );

    const tagsWithoutIdentifierAttributes = removeTagsMissingIdentifierAttributes(
      tagsWithOneMissingAnIdentifierAttribute
    );

    assert.equal(
      tagsWithoutIdentifierAttributes.length,
      1,
      'Expected only tags to be in the list tagsWithoutIdentifierAttributes'
    );
    assert.equal(
      tagsWithoutIdentifierAttributes[0].identifierField,
      'testFieldA',
      'Expected tag with identifierField testFieldA to be a tag with complete identifier attributes i.e. identifierField & identifierType'
    );
  });

  test('reading the compliance-info working copy with readonly annotations', function(assert): void {
    const rawComplianceInfoWithReadonlyAnnotations = { ...rawComplianceInfo, complianceEntities };
    const complianceInfo = new DatasetComplianceInfo(rawComplianceInfoWithReadonlyAnnotations, []);
    // Since we aren't testing for behavior when schemaFields are specified,
    // instantiation here omits adding the columns field so schemaFields will be unavailable and
    // the annotations will be returned without the process of filtering to the dataset schema
    const emptySchema = new DatasetSchema({ schemaless: false, rawSchema: null, keySchema: null });

    const { complianceEntities: annotations } = complianceInfo.readWorkingCopy({
      withoutNullFields: true,
      schema: emptySchema
    });

    assert.equal(annotations.length, 1, 'Expected the compliance info working copy to have only one field annotation');

    const [{ readonly, identifierField }] = annotations;

    assert.equal(readonly, undefined, "Expected the annotation's readonly field to not exist on the result");
    assert.equal(identifierField, 'testFieldB', 'Expected testFieldB to be the compliance info workingCopy result');
  });

  test('working copy for compliance info properties', function(assert): void {
    const complianceInfo = new DatasetComplianceInfo(rawComplianceInfo, []);

    assert.equal(complianceInfo.confidentiality, null, 'Confidentiality is derived from raw info as expected');
    assert.equal(
      complianceInfo.containingPersonalData,
      null,
      'Personal data flag is derived from raw info as expected'
    );

    complianceInfo.updateWorkingConfidentiality(Classification.Public);
    complianceInfo.updateWorkingContainingPersonalData(false);
    assert.equal(complianceInfo.confidentiality, Classification.Public, 'Update function works as expected');
    assert.equal(
      complianceInfo.containingPersonalData,
      false,
      'Update function updates personal info flag as expected'
    );

    complianceInfo.createWorkingCopy();
    assert.equal(complianceInfo.confidentiality, null, 'After reset, confidentiality returns to derived value');
    assert.equal(
      complianceInfo.containingPersonalData,
      null,
      'After reset, personal data flag returns to derived value'
    );
  });
});
