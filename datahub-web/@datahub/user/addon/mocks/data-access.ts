import { FakeDatasetEntity } from '@datahub/user/mocks/models/dataset-entity';
import { AclAccess } from '@datahub/data-models/entity/person/modules/acl';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { set } from '@ember/object';
import { AccessControlAccessType, AclAccessStatus } from '@datahub/data-models/constants/entity/common/acl-access';

// TODO: [META-9851] Temporary home for this function for integrated dev/demo, should be moved to test/dummy folder
/**
 * Creates a mock data access entity so that we can use it in our dummy app for testing
 */
export const generateMockDataAccessForDatasets = (): Array<AclAccess<DatasetEntity>> => {
  const datasetEntityA = new FakeDatasetEntity('pikachu');
  const datasetEntityB = new FakeDatasetEntity('eevee');

  set(datasetEntityA, 'name', 'pikachu');
  set(datasetEntityB, 'name', 'eevee');

  return [
    new AclAccess(datasetEntityA, {
      environment: 'HOLDEM/WAR',
      accessType: [AccessControlAccessType.Read],
      status: AclAccessStatus.EXPIRED,
      expiration: 1552953600000,
      businessJustification: 'Need jit acl access to catch em all'
    }),
    new AclAccess(datasetEntityB, {
      environment: 'CORP',
      accessType: [AccessControlAccessType.Read, AccessControlAccessType.Write],
      status: AclAccessStatus.ACTIVE,
      expiration: 1618790400000,
      businessJustification: 'Need jit acl access to catch em all'
    })
  ];
};
