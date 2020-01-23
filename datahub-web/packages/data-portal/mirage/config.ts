import { faker } from 'ember-cli-mirage';
import { IMirageRequest, IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { ApiStatus } from '@datahub/utils/api/shared';
import { getDatasetSchema } from 'wherehows-web/mirage/helpers/columns';
import { getDatasetOwners } from 'wherehows-web/mirage/helpers/dataset-owners';
import { getDatasetPlatforms } from 'wherehows-web/mirage/helpers/dataset-platforms';
import { getDatasetView } from 'wherehows-web/mirage/helpers/dataset-view';
import { getOwnerTypes } from 'wherehows-web/mirage/helpers/owner-types';
import { getConfig } from 'wherehows-web/mirage/helpers/config';
import { getAuth } from 'wherehows-web/mirage/helpers/authenticate';
import { aclAuth } from 'wherehows-web/mirage/helpers/aclauth';
import { getDatasetUpstreams } from 'wherehows-web/mirage/helpers/dataset-upstreams';
import { getDatasetFabrics } from 'wherehows-web/mirage/helpers/dataset-fabrics';
import { getDatasetCount } from 'wherehows-web/mirage/helpers/dataset-count';
import { getDatasetDownstreams } from 'wherehows-web/mirage/helpers/dataset-downstreams';
import { getBrowsePlatforms } from 'wherehows-web/mirage/helpers/browse-platforms';
import { getBrowsePlatform } from 'wherehows-web/mirage/helpers/browse-platform';
import { getSearchResults } from 'wherehows-web/mirage/helpers/search';
import { getAutocompleteDatasets, getAutocomplete } from 'wherehows-web/mirage/helpers/autocomplete';
import { getDatasetOwnerSuggestion } from './helpers/owner-suggestions';
import { getDatasetSnapshot } from './helpers/dataset-snapshot';
import { browse } from 'wherehows-web/mirage/helpers/browse';
import searchResponse from 'wherehows-web/mirage/fixtures/search-response';
import { getSamplePageViewResponse } from 'wherehows-web/mirage/helpers/search/pageview-response';
import { getEntitySearchResults } from 'wherehows-web/mirage/helpers/search/entity';
import { browsePaths } from 'wherehows-web/mirage/helpers/browse-paths';

export default function(this: IMirageServer): void {
  this.get('/config', getConfig);

  this.post('/authenticate', getAuth);

  this.passthrough('/write-coverage');

  this.namespace = '/api/v2';

  this.get('/autocomplete', getAutocomplete);

  this.get('/browse', browse);

  this.get('/browsePaths', browsePaths);

  this.get('/datasets/:identifier/', getDatasetView);

  this.get('/datasets/:identifier/owners', getDatasetOwners);

  this.get('/datasets/:dataset_id/owners/suggestion', getDatasetOwnerSuggestion);

  this.get('/datasets/:dataset_id/schema', getDatasetSchema);

  this.get('/datasets/:dataset_id/owners', getDatasetOwners);

  this.get('/datasets/:dataset_id/acl', () => []);

  this.get('/list/platforms', getDatasetPlatforms);

  this.get('/platforms', getBrowsePlatforms);

  this.get('/platforms/:platform', getBrowsePlatform);

  this.get('/platforms/:platform/prefix/:prefix', getBrowsePlatform);

  this.get('/datasets/:dataset_id/upstreams', getDatasetUpstreams);

  this.get('/datasets/:dataset_id/downstreams', getDatasetDownstreams);

  this.get('/datasets/:dataset_id/fabrics', getDatasetFabrics);

  this.get('/datasets/:dataset_id/snapshot', getDatasetSnapshot);

  this.get('/datasets/count/platform/:platform_id/prefix/:prefix_id', getDatasetCount);

  this.get('/datasets/count/platform/:platform_id', getDatasetCount);

  this.get('/datasets/:dataset_id/dataorigins', () => ({
    dataOrigins: [{ displayTitle: 'PROD', origin: 'PROD' }, { displayTitle: 'EI', origin: 'EI' }]
  }));

  this.get('/search', getEntitySearchResults);

  this.namespace = '/api/v1';

  this.get('/owner/types', getOwnerTypes);

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

  this.get('/search', (schema: any, fakeRequest: IMirageRequest) => {
    if (fakeRequest.queryParams) {
      const { keyword, entity } = fakeRequest.queryParams;

      if (entity === 'datasets' && keyword === searchResponse.result.keywords) {
        return getSamplePageViewResponse();
      }
    }

    return getSearchResults(schema, fakeRequest);
  });

  this.get('/autocomplete/datasets', getAutocompleteDatasets);
}

export function testConfig(this: IMirageServer): void {}
