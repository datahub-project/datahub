import { faker } from 'ember-cli-mirage';
import { getDatasetColumns } from 'wherehows-web/mirage/helpers/columns';
import { getDatasetCompliance } from 'wherehows-web/mirage/helpers/compliance';
import { getComplianceDataTypes } from 'wherehows-web/mirage/helpers/compliance-data-types';
import { getDatasetComplianceSuggestion } from 'wherehows-web/mirage/helpers/compliance-suggestions';
import { getDataset } from 'wherehows-web/mirage/helpers/dataset';
import { getDatasetAccess } from 'wherehows-web/mirage/helpers/dataset-access';
import { getDatasetComments } from 'wherehows-web/mirage/helpers/dataset-comments';
import { getDatasetDbVersions } from 'wherehows-web/mirage/helpers/dataset-db-versions';
import { getDatasetDepends } from 'wherehows-web/mirage/helpers/dataset-depends';
import { getDatasetImpact } from 'wherehows-web/mirage/helpers/dataset-impact';
import { getDatasetInstances } from 'wherehows-web/mirage/helpers/dataset-instances';
import { getDatasetOwners } from 'wherehows-web/mirage/helpers/dataset-owners';
import { getDatasetPlatforms } from 'wherehows-web/mirage/helpers/dataset-platforms';
import { getDatasetProperties } from 'wherehows-web/mirage/helpers/dataset-properties';
import { getDatasetReferences } from 'wherehows-web/mirage/helpers/dataset-references';
import { getDatasetSample } from 'wherehows-web/mirage/helpers/dataset-sample';
import { getDatasetView } from 'wherehows-web/mirage/helpers/dataset-view';
import { getOwnerTypes } from 'wherehows-web/mirage/helpers/owner-types';
import { IFunctionRouteHandler, IMirageServer } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';
import { getConfig } from 'wherehows-web/mirage/helpers/config';
import { getAuth } from 'wherehows-web/mirage/helpers/authenticate';
import { aclAuth } from 'wherehows-web/mirage/helpers/aclauth';

export default function(this: IMirageServer) {
  this.get('/config', getConfig);

  this.post('/authenticate', getAuth);

  this.passthrough('/write-coverage');

  this.namespace = '/api/v2';

  this.get('/list/complianceDataTypes', getComplianceDataTypes);

  this.get('/list/platforms', getDatasetPlatforms);

  this.namespace = '/api/v1';

  this.get('/datasets/:dataset_id', getDataset);

  this.get('/datasets/:dataset_id/view', getDatasetView);

  this.get('/datasets/:dataset_id/columns', getDatasetColumns);

  this.get('/datasets/:dataset_id/compliance', getDatasetCompliance);

  this.get('/datasets/:dataset_id/compliance/suggestions', getDatasetComplianceSuggestion);

  this.get('/datasets/:dataset_id/comments', getDatasetComments);

  this.get('/datasets/:dataset_id/owners', getDatasetOwners);

  this.get('/datasets/:dataset_id/instances', getDatasetInstances);

  this.get('/owner/types', getOwnerTypes);

  this.get('/datasets/:dataset_id/sample', getDatasetSample);

  this.get('/datasets/:dataset_id/impacts', getDatasetImpact);

  this.get('/datasets/:dataset_id/depends', getDatasetDepends);

  this.get('/datasets/:dataset_id/access', getDatasetAccess);

  this.get('/datasets/:dataset_id/references', getDatasetReferences);

  this.get('/datasets/:dataset_id/properties', getDatasetProperties);

  this.get('/datasets/:dataset_id/versions/db/:db_id', getDatasetDbVersions);

  interface IFlowsObject {
    flows: any;
  }

  this.get('/flows', function(this: IFunctionRouteHandler, { flows }: IFlowsObject, request: any) {
    const { page } = request.queryParams;
    const flowsArr = this.serialize(flows.all());
    const count = faker.random.number({ min: 20000, max: 40000 });
    const itemsPerPage = 10;

    return {
      status: ApiStatus.OK,
      data: {
        count: count,
        flows: flowsArr,
        itemsPerPage: itemsPerPage,
        page: page,
        totalPages: Math.round(count / itemsPerPage)
      }
    };
  });

  this.get('/list/datasets', (server: any) => {
    const { datasetNodes } = server.db;

    return {
      status: 'ok',
      nodes: datasetNodes
    };
  });

  interface IOwnersObject {
    owners: any;
  }
  interface IDatasetsObject {
    datasets: any;
  }
  this.get('/datasets', function(
    this: IFunctionRouteHandler,
    { datasets }: IDatasetsObject,
    { owners }: IOwnersObject,
    request: any
  ) {
    const { page } = request.queryParams;
    const datasetsArr = this.serialize(datasets.all());
    const ownersArr = this.serialize(owners.all());
    const newDatasetsArr = datasetsArr.map(function(dataset: any) {
      dataset.owners = ownersArr;
      return dataset;
    });

    const count = faker.random.number({ min: 20000, max: 40000 });
    const itemsPerPage = 10;
    return {
      status: ApiStatus.OK,
      data: {
        count: count,
        page: page,
        itemsPerPage: itemsPerPage,
        totalPages: Math.round(count / itemsPerPage),
        datasets: newDatasetsArr
      }
    };
  });

  this.get('/metrics', (server: any, request: any) => {
    const { page } = request.queryParams;
    const { metricMetrics } = server.db;
    const count = faker.random.number({ min: 10000, max: 20000 });
    const itemsPerPage = 10;

    return {
      status: ApiStatus.OK,
      data: {
        count: count,
        page: page,
        itemsPerPage: itemsPerPage,
        totalPages: Math.round(count / itemsPerPage),
        metrics: metricMetrics
      }
    };
  });

  this.get('/party/entities', (server: any) => {
    const { userEntities } = server.db;

    return {
      status: ApiStatus.OK,
      userEntities: userEntities
    };
  });

  this.get('/user/me', () => {
    const testUser = 'testUser';
    return {
      user: {
        id: faker.random.number({ min: 1000, max: 5000 }),
        userName: testUser,
        departmentNum: 0,
        email: testUser + '@linkedin.com',
        name: testUser,
        userSetting: <{ [prop: string]: null }>{
          detailDefaultView: null,
          defaultWatch: null
        }
      },
      status: ApiStatus.OK
    };
  });

  /**
   * Add GET request to check the current user ACL permission
   */
  this.get('/acl', (server: any, request: any) => {
    if (request.queryParams.hasOwnProperty('LDAP')) {
      const { LDAP } = request.queryParams;
      const principal = `urn:li:userPrincipal:${LDAP}`;
      let accessUserslist = server.db.datasetAclUsers.where({ principal });

      if (accessUserslist.length > 0) {
        return {
          status: ApiStatus.OK,
          isAccess: true,
          body: server.db.datasetAclUsers
        };
      }
      return {
        status: ApiStatus.FAILED,
        isAccess: false,
        body: server.db.datasetAclUsers
      };
    } else {
      return {
        users: server.db.datasetAclUsers,
        status: ApiStatus.OK
      };
    }
  });

  /**
   * Add POST request to support to the current user get ACL permission.
   */
  this.post('/acl', aclAuth);

  this.passthrough();
}

export function testConfig(this: IMirageServer) {}
