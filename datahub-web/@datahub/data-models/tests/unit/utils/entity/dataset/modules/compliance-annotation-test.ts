import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';
import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { ComplianceFieldIdValue } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';
import { set } from '@ember/object';

module('Unit | Utility | entity/dataset/modules/compliance-annotation', function(hooks): void {
  setupTest(hooks);

  test('DatasetComplianceAnnotation class', function(assert): void {
    const complianceAnnotationA = new DatasetComplianceAnnotation();
    assert.equal(complianceAnnotationA.identifierField, '', 'Class works with blank base case');

    const complianceAnnotationB = new DatasetComplianceAnnotation(undefined, 'detective pikachu');
    assert.equal(
      complianceAnnotationB.identifierField,
      'detective pikachu',
      'Class can assign field name without being given a tag object'
    );
    assert.equal(complianceAnnotationB.identifierType, null, 'Correctly assigns base values when not given tag object');

    const complianceAnnotationC = new DatasetComplianceAnnotation({
      identifierField: 'pikachu_tag',
      identifierType: ComplianceFieldIdValue.MemberId,
      logicalType: null,
      nonOwner: false,
      pii: true,
      readonly: false,
      securityClassification: null,
      valuePattern: ''
    });

    assert.equal(
      complianceAnnotationC.identifierType,
      ComplianceFieldIdValue.MemberId,
      'Correctly assigns identifier type in initialization'
    );

    set(complianceAnnotationC, 'identifierType', ComplianceFieldIdValue.GroupId);
    assert.ok(
      complianceAnnotationC.readWorkingCopy().identifierType !== ComplianceFieldIdValue.MemberId,
      'Tests readWorkingCopy and value has changed when set anew'
    );
    assert.ok(
      complianceAnnotationC.readWorkingCopy().identifierType === ComplianceFieldIdValue.GroupId,
      'Setting a new identifier type persists'
    );

    complianceAnnotationC.resetWorkingCopy();
    assert.ok(
      complianceAnnotationC.readWorkingCopy().identifierType === ComplianceFieldIdValue.MemberId,
      'Reset working copy works as expected'
    );
  });
});
