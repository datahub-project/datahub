import Route from '@ember/routing/route';
import { populateMockPersonEntity } from '@datahub/user/mocks/person-entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { AclAccess } from '@datahub/data-models/entity/person/modules/acl';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { generateMockDataAccessForDatasets } from 'dummy/mocks/data-access';

export default class DataAccess extends Route {
  model(): {
    accessList: Array<AclAccess<DatasetEntity>>;
    userEntity: PersonEntity;
  } {
    return {
      accessList: generateMockDataAccessForDatasets(),
      userEntity: populateMockPersonEntity(new PersonEntity('pikachu'))
    };
  }
}
