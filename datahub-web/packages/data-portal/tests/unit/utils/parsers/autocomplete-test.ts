import { module, test } from 'qunit';
import { grammarProcessingSteps, typeaheadQueryProcessor } from 'wherehows-web/utils/parsers/autocomplete';
import { startMirage } from 'wherehows-web/initializers/ember-cli-mirage';
import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageWherehows } from 'wherehows-web/typings/ember-cli-mirage';
import { ISuggestionGroup } from 'wherehows-web/utils/parsers/autocomplete/types';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { DataModelName } from '@datahub/data-models/constants/entity';

interface ITestSet {
  entity: DataModelName;
  description: string;
  text: string;
  results: Array<ISuggestionGroup>;
}

const createTests = (server: IMirageWherehows): Array<ITestSet> => {
  server.create('datasetView', { name: 'platform' });
  server.create('datasetView', { name: 'pageviewevent' });
  server.create('platform', { name: 'hive' });
  server.create('platform', { name: 'mysql' });

  return [
    {
      entity: DatasetEntity.displayName,
      description: 'Initial suggestions',
      text: '',
      results: [
        {
          groupName: 'Datasets',
          options: [
            {
              disabled: true,
              text: '',
              title: 'type at least 3 more characters to see Datasets names'
            }
          ]
        },
        {
          groupName: 'Filter By',
          options: [
            {
              description: 'The origin of the dataset, e.g.: origin:PROD',
              text: 'origin:',
              title: 'origin:'
            },
            {
              description: 'The name of the dataset, e.g.: name:TRACKING.PageViewEvent',
              text: 'name:',
              title: 'name:'
            },
            {
              description: 'The confirmed owners for the dataset, e.g.: owners:jweiner',
              text: 'owners:',
              title: 'owners:'
            },
            {
              description: 'The platform of the dataset, e.g.: platform:kafka',
              text: 'platform:',
              title: 'platform:'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: '1 Dataset',
      text: 'pageview',
      results: [
        {
          groupName: 'Datasets',
          options: [
            {
              description: '',
              text: 'pageviewevent ',
              title: 'pageviewevent'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Dataset with filter 1',
      text: 'pageview AND platfo',
      results: [
        {
          groupName: 'Filter By',
          options: [
            {
              description: 'The platform of the dataset, e.g.: platform:kafka',
              text: 'pageview AND platform:',
              title: 'platform:'
            }
          ]
        },
        {
          groupName: 'Datasets',
          options: [
            {
              description: '',
              text: 'pageview AND platform ',
              title: 'platform'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Dataset with filter 2',
      text: 'pageview AND platform',
      results: [
        {
          groupName: 'Filter By',
          options: [
            {
              description: 'The platform of the dataset, e.g.: platform:kafka',
              text: 'pageview AND platform:',
              title: 'platform:'
            }
          ]
        },
        {
          groupName: 'Datasets',
          options: [
            {
              description: '',
              text: 'pageview AND platform ',
              title: 'platform'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Dataset with filter 3',
      text: 'pageview AND name:pageview',
      results: [
        {
          groupName: 'Filter By',
          options: [
            {
              text: 'pageview AND name:pageviewevent ',
              title: 'name:pageviewevent'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Dataset with filter platform',
      text: 'platform:',
      results: [
        {
          groupName: 'Filter By',
          options: [
            {
              text: 'platform:hive ',
              title: 'platform:hive'
            },
            {
              text: 'platform:mysql ',
              title: 'platform:mysql'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Dataset with filter platform my',
      text: 'platform:my',
      results: [
        {
          groupName: 'Filter By',
          options: [
            {
              text: 'platform:mysql ',
              title: 'platform:mysql'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Dataset with filter fabric',
      text: 'origin:co',
      results: [
        {
          groupName: 'Filter By',
          options: [
            {
              text: 'dataorigin:corp ',
              title: 'dataorigin:corp'
            },
            {
              text: 'dataorigin:azurecontrol ',
              title: 'dataorigin:azurecontrol'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Logical Operators',
      text: 'something AN',
      results: [
        {
          groupName: 'Datasets',
          options: [
            {
              disabled: true,
              text: '',
              title: 'type at least 1 more characters to see Datasets names'
            }
          ]
        },
        {
          groupName: 'Operators',
          options: [
            {
              text: 'something AND ',
              title: 'AND'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Logical Operators',
      text: 'something O',
      results: [
        {
          groupName: 'Datasets',
          options: [
            {
              disabled: true,
              text: '',
              title: 'type at least 2 more characters to see Datasets names'
            }
          ]
        },
        {
          groupName: 'Operators',
          options: [
            {
              text: 'something OR ',
              title: 'OR'
            }
          ]
        }
      ]
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Invalid Syntax',
      text: 'notreallyafacet:',
      results: []
    },
    {
      entity: DatasetEntity.displayName,
      description: 'Next things',
      text: 'something ',
      results: [
        {
          groupName: 'Datasets',
          options: [
            {
              disabled: true,
              text: '',
              title: 'type at least 3 more characters to see Datasets names'
            }
          ]
        },
        {
          groupName: 'Operators',
          options: [
            {
              text: 'something AND ',
              title: 'AND'
            },
            {
              text: 'something OR ',
              title: 'OR'
            }
          ]
        },
        {
          groupName: 'Filter By',
          options: [
            {
              description: 'The origin of the dataset, e.g.: origin:PROD',
              text: 'something origin:',
              title: 'origin:'
            },
            {
              description: 'The name of the dataset, e.g.: name:TRACKING.PageViewEvent',
              text: 'something name:',
              title: 'name:'
            },
            {
              description: 'The confirmed owners for the dataset, e.g.: owners:jweiner',
              text: 'something owners:',
              title: 'owners:'
            },
            {
              description: 'The platform of the dataset, e.g.: platform:kafka',
              text: 'something platform:',
              title: 'platform:'
            }
          ]
        }
      ]
    }
  ];
};

module('Unit | Utility | Autocomplete Suggestions', function(hooks) {
  let server: IMirageServer;
  hooks.beforeEach(function() {
    server = startMirage();
  });

  hooks.afterEach(function() {
    server.shutdown();
  });

  test('Suggestions returns as expected', async function(assert) {
    const tests = createTests(server);

    assert.expect(tests.length);

    // tests must be resolved in sequence since processing includes some debouncing
    await tests.reduce(async (previousResolution: Promise<void>, myTest: ITestSet): Promise<void> => {
      await previousResolution;
      const result = await typeaheadQueryProcessor(myTest.text, myTest.entity, grammarProcessingSteps);

      assert.deepEqual(result, myTest.results, myTest.description);
    }, Promise.resolve());
  });
});
