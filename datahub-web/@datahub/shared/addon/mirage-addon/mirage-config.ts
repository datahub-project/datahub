import { Server } from 'ember-cli-mirage';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import { getTopConsumers } from '@datahub/shared/mirage-addon/test-helpers/top-consumers';
import { getEntityConfigs } from '@datahub/shared/mirage-addon/test-helpers/entity-configs';
import { healthEndpoint } from '@datahub/shared/api/health';
import { getEntityHealth } from '@datahub/shared/mirage-addon/test-helpers/entity-health/health-metadata';
import { changeLogEndpoint } from '@datahub/shared/api/change-management/change-log';
import { getChangeLog } from '@datahub/shared/mirage-addon/test-helpers/change-log';
import { browse } from '@datahub/shared/mirage-addon/helpers/browse';
import { browsePaths } from '@datahub/shared/mirage-addon/helpers/browse-paths';
import { getConfig } from '@datahub/shared/mirage-addon/helpers/config';
import { getAuth } from '@datahub/shared/mirage-addon/helpers/authenticate';

/**
 * Datahub shared's mirage setup helper to augment the server with shorthand request hooks
 * @param {Server} server the Mirage server instance
 */
export const setup = (server: Server): void => {
  server.namespace = '';
  server.get('/config', getConfig);
  server.post('/authenticate', getAuth);
  server.passthrough('/write-coverage');

  server.namespace = getApiRoot(ApiVersion.v2);

  server.get('/browse', browse);

  server.get('/browsePaths', browsePaths);

  // Routes for entity insights
  server.get('datasets/:urn/top-consumers', getTopConsumers);
  // Route declared for instantiating person entity for top consumers insight
  server.get('corpusers/:urn', () => {});

  // Routes for entity social features
  server.get(
    `datasets/:urn/likes`,
    (): Com.Linkedin.Common.Likes => {
      return { actions: [{ likedBy: 'aketchum' }, { likedBy: 'misty' }, { likedBy: 'brock' }] };
    }
  );

  // TODO : Add follows in mirage : https://jira01.corp.linkedin.com:8443/browse/META-11926
  server.get(
    `datasets/:urn/follows`,
    (): Com.Linkedin.Common.Follow => ({
      followers: [
        { follower: { corpUser: 'aketchum' } },
        { follower: { corpUser: 'misty' } },
        { follower: { corpUser: 'brock' } }
      ]
    })
  );

  server.get(`:entity/:urn/${healthEndpoint}`, getEntityHealth);

  // Entity configs for configurable features
  server.get('/entity-configs/:urn', getEntityConfigs);

  server.get(
    '/lineage/graph/:urn',
    (): Com.Linkedin.Metadata.Graph.Graph => ({
      nodes: [],
      edges: []
    })
  );

  server.get(`${changeLogEndpoint}/:id`, getChangeLog);

  server.namespace = 'api/v2';
  // TODO: [META-11379] Replace with factory once we have mirage setup for person entity
  server.get('/user/me', () => {
    const testUser = 'testUser';
    return {
      username: testUser,
      info: {
        departmentName: 'APA',
        lastName: 'User',
        firstName: 'Test',
        active: true,
        fullName: 'Test User',
        title: 'Software Engineer',
        managerUrn: 'urn:li:corpuser:testManager',
        managerName: 'Test Manager',
        email: 'testuser@linkedin.com'
      }
    };
  });
};
