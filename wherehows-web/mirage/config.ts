import { faker } from 'ember-cli-mirage';
import { IFunctionRouteHandler, IMirageServer } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';
import { getConfig } from 'wherehows-web/mirage/helpers/config';
import { getAuth } from 'wherehows-web/mirage/helpers/authenticate';

export default function(this: IMirageServer) {
  this.get('/config', getConfig);

  this.post('/authenticate', getAuth);

  this.passthrough('/write-coverage');

  this.namespace = '/api/v1';

  this.get('/list/complianceDataTypes', function(
    this: IFunctionRouteHandler,
    { complianceDataTypes }: { complianceDataTypes: any }
  ) {
    return {
      complianceDataTypes: this.serialize(complianceDataTypes.all()),
      status: ApiStatus.OK
    };
  });

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
        userSetting: {
          detailDefaultView: null,
          defaultWatch: null
        }
      },
      status: ApiStatus.OK
    };
  });

  this.passthrough();
}

export function testConfig(this: IMirageServer) {}
