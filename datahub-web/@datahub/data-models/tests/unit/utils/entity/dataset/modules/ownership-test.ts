import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { set } from '@ember/object';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import DatasetOwnership from '@datahub/data-models/entity/dataset/modules/ownership';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { testDatasetOwnershipUrn } from '@datahub/data-models/mirage-addon/test-helpers/datasets/ownership';
import ownershipScenario from '@datahub/data-models/mirage-addon/scenarios/dataset-ownership';
import ownersForTestUrn from '@datahub/data-models/mirage-addon/fixtures/dataset-ownership';
import { IOwner } from '@datahub/data-models/types/entity/dataset/ownership';
import { fromLegacy } from '@datahub/data-models/entity/dataset/utils/legacy';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';

module('Unit | Utility | entity/dataset/modules/ownership', function(hooks): void {
  setupTest(hooks);
  setupMirage(hooks);

  const datasetEntity = new DatasetEntity(
    testDatasetOwnershipUrn,
    fromLegacy({
      fabric: FabricType.CORP,
      platform: DatasetPlatform.HDFS,
      createdTime: Date.now(),
      modifiedTime: Date.now(),
      description: '',
      nativeName: '',
      nativeType: '',
      uri: '',
      tags: [],
      removed: false,
      decommissionTime: null,
      deprecated: null,
      deprecationNote: null,
      properties: null,
      healthScore: Math.random()
    })
  );

  test('DatasetOwnership instantiation', function(assert): void {
    const datasetOwnership = new DatasetOwnership(datasetEntity);

    assert.equal(
      datasetOwnership.entity,
      datasetEntity,
      'Expected DatasetOwnership to reference the Dataset on the entity class field'
    );
    assert.equal(
      datasetOwnership.urn,
      testDatasetOwnershipUrn,
      'Expected DatasetOwnership to reference the Dataset urn property'
    );
  });

  test('DatasetOwnership behavior post build', async function(this: MirageTestContext, assert): Promise<void> {
    ownershipScenario(this.server);

    const datasetOwnership = new DatasetOwnership(datasetEntity);
    const builtDatasetOwnership = await datasetOwnership.build();
    const [firstOwner] = ownersForTestUrn;
    const { userName } = firstOwner;

    assert.equal(
      builtDatasetOwnership,
      datasetOwnership,
      'Expected DatasetOwnership#build() invocation, to resolve with the same instance'
    );

    assert.equal(
      builtDatasetOwnership.owners.length,
      ownersForTestUrn.length,
      'Expected number of owners in DatasetOwnership to match fixture length'
    );

    assert.ok(builtDatasetOwnership.isOwner(userName), `Expected DatasetOwnership to confirm ${userName} as an owner`);

    assert.ok(builtDatasetOwnership.isValid, 'Expected DatasetOwnership to be valid for fixture owners');

    set(builtDatasetOwnership, 'owners', []);
    assert.notOk(builtDatasetOwnership.isValid, 'Expected DatasetOwnership to be invalid when no owner exists');

    const invalidNumberOfOwners = Array.from(
      { length: DatasetOwnership.minRequiredConfirmedOwners - 1 },
      (): IOwner => firstOwner
    );

    set(builtDatasetOwnership, 'owners', invalidNumberOfOwners);
    assert.notOk(
      builtDatasetOwnership.isValid,
      `Expected DatasetOwnership to be invalid when the number of owners is less than the required: ${DatasetOwnership.minRequiredConfirmedOwners}`
    );

    set(builtDatasetOwnership, 'owners', [{ ...firstOwner, confirmedBy: null }, firstOwner]);
    assert.notOk(
      builtDatasetOwnership.isValid,
      'Expected DatasetOwnership to be invalid when the number of confirmed owners is less than the required'
    );
    set(builtDatasetOwnership, 'owners', [{ ...firstOwner, isActive: false }, firstOwner]);
    assert.notOk(
      builtDatasetOwnership.isValid,
      'Expected DatasetOwnership to be invalid when the number of active owners is less than the required'
    );
    set(builtDatasetOwnership, 'owners', [{ ...firstOwner, idType: 'GROUP' }, firstOwner]);
    assert.notOk(
      builtDatasetOwnership.isValid,
      'Expected DatasetOwnership to be invalid when the number of owners with USER idType is less than the required'
    );
    set(builtDatasetOwnership, 'owners', [{ ...firstOwner, type: 'STAKEHOLDER' }, firstOwner]);
    assert.notOk(
      builtDatasetOwnership.isValid,
      'Expected DatasetOwnership to be invalid when the number of owners with DATA_OWNER type is less than the required'
    );
  });
});
