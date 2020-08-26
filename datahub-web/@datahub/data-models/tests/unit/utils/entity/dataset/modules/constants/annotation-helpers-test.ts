import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';
import { setProperties } from '@ember/object';
import {
  annotationIsValid,
  applyAnnotationsByEditableProperties
} from '@datahub/data-models/entity/dataset/modules/constants/annotation-helpers';
import {
  ComplianceFieldIdValue,
  MemberIdLogicalType
} from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';

module('Unit | Utility | entity/dataset/modules/constants/annotation-helpers', function(hooks): void {
  setupTest(hooks);

  test('Dataset compliance annotation helpers test: annotationIsValid', function(assert): void {
    const annotationTag = new DatasetComplianceAnnotation();
    assert.notOk(annotationIsValid(annotationTag), 'Returns false for an empty identifier field');

    setProperties(annotationTag, { identifierType: ComplianceFieldIdValue.None });
    assert.ok(annotationIsValid(annotationTag), 'A none tag should be considered valid without a logical type');

    setProperties(annotationTag, {
      identifierType: ComplianceFieldIdValue.GroupId,
      logicalType: MemberIdLogicalType.Numeric,
      isPurgeKey: false
    });
    assert.ok(annotationIsValid(annotationTag), 'A member id tag should have a logical type and purge key notation');

    setProperties(annotationTag, { logicalType: undefined, isPurgeKey: undefined });
    assert.notOk(
      annotationIsValid(annotationTag),
      'A member id tag should have a logical type and is not considered valid if missing'
    );

    setProperties(annotationTag, {
      logicalType: MemberIdLogicalType.Urn,
      isPurgeKey: false
    });
    assert.ok(annotationIsValid(annotationTag), 'A false purgeKey is not the same as an undefined purgeKey');

    setProperties(annotationTag, { logicalType: MemberIdLogicalType.Custom });
    assert.notOk(annotationIsValid(annotationTag), 'A custom logical type should have a value pattern');

    setProperties(annotationTag, { valuePattern: 'totally regex pattern' });
    assert.ok(annotationIsValid(annotationTag), 'A custom logical type with an existing value pattern will pass');
  });

  test('Dataset compliance annotation helpers test: applyAnnotationsByEditableProperties', function(assert): void {
    const propTags = [
      {
        identifierField: 'pikachu',
        identifierType: ComplianceFieldIdValue.MemberId,
        logicalType: MemberIdLogicalType.Numeric,
        isPurgeKey: false,
        valuePattern: undefined,
        isReadOnly: false
      },
      {
        identifierField: 'eevee',
        identifierType: ComplianceFieldIdValue.None,
        logicalType: undefined,
        isPurgeKey: false,
        valuePattern: undefined,
        isReadOnly: false
      }
    ];

    const result = applyAnnotationsByEditableProperties(propTags);

    assert.equal(result.length, 2, 'Function returns expected number of elements');
    result.forEach(
      item => assert.ok(item instanceof DatasetComplianceAnnotation),
      'Function returns expected instances of tag'
    );

    const secondResult = result[1];
    const secondPropTag = propTags[1];

    assert.equal(secondResult.identifierType, secondPropTag.identifierType, 'Values applied properly');
    assert.equal(secondResult.valuePattern, secondPropTag.valuePattern, 'Sanity check, values appleid prpoerly');
  });
});
