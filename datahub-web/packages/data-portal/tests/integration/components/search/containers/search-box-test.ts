import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import { getText } from '@datahub/utils/test-helpers/dom-helpers';
import hbs from 'htmlbars-inline-precompile';
import Component from '@ember/component';
import { didCancel, Task } from 'ember-concurrency';
import { TestContext } from 'ember-test-helpers';
import { setMockConfig, resetConfig } from 'wherehows-web/services/configurator';
import SearchBox from 'wherehows-web/components/search/search-box';
import { ISuggestion, ISuggestionGroup } from 'wherehows-web/utils/parsers/autocomplete/types';
import { IMirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { DataModelName } from '@datahub/data-models/constants/entity';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

const getMirageHandle = (test: IMirageTestContext, api: string, verb: string): { numberOfCalls: number } => {
  // @ts-ignore
  return test.server.pretender.hosts.forURL(api)[verb.toLocaleUpperCase()].recognize(api)[0].handler;
};

const containerComponentTest = (test: TestContext, testFn: (me: Component) => void): void => {
  test.owner.register(
    'component:container-stub',
    class ContainerStub extends Component {
      didInsertElement(): void {
        testFn(this);
      }
    }
  );
};

/**
 * Dynamic reference to the SearchBox component's onTypeaheadTask's return type
 * @alias
 */
type SearchBoxTypeaheadTaskInstance = ReturnType<SearchBox['onTypeaheadTask']>;

interface IContainerStub extends Component {
  onTypeahead: Task<Array<string>, (word: string, entity: DataModelName) => SearchBoxTypeaheadTaskInstance>;
}

const getFirstSuggestion = async (task: SearchBoxTypeaheadTaskInstance): Promise<ISuggestion | void> => {
  try {
    // IterableIterator is not assignable to Promise and has no overlap
    const [group] = await ((task as unknown) as Array<ISuggestionGroup>);
    const [suggestion] = group.options;
    return suggestion;
  } catch (e) {
    if (!didCancel(e)) {
      throw e;
    }
  }
};

module('Integration | Component | search/containers/search-box', function(hooks): void {
  setupRenderingTest(hooks);

  hooks.beforeEach(function(): void {
    setMockConfig({});
  });

  hooks.afterEach(function(): void {
    resetConfig();
  });

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`
      {{#search/containers/search-box  as |keyword entity onTypeahead onSearch|}}
        template block text
      {{/search/containers/search-box}}
    `);

    assert.equal(getText(this).trim(), 'template block text');
  });

  test('onTypeahead', async function(this: IMirageTestContext, assert): Promise<void> {
    const apiHandler = getMirageHandle(this, '/api/v2/autocomplete', 'get');
    assert.expect(6);

    containerComponentTest(
      this,
      async (component: IContainerStub): Promise<void> => {
        const datasetProps = { name: 'hola nacho' };
        this.server.create('datasetView', datasetProps);

        // dont return anything with less than 3
        const result1 = await getFirstSuggestion(component.onTypeahead.perform('h', DatasetEntity.displayName));
        assert.equal(
          result1 && result1.title,
          'type at least 2 more characters to see Datasets names',
          'expected suggestion to prompt for more input'
        );

        // return list
        const results2 = await getFirstSuggestion(component.onTypeahead.perform('hol', DatasetEntity.displayName));
        assert.equal(results2 && results2.title, datasetProps.name, 'expected suggestion title to match dataset');

        // cache return
        const results3 = await getFirstSuggestion(component.onTypeahead.perform('hol', DatasetEntity.displayName));
        assert.equal(results3 && results3.title, datasetProps.name, 'expected suggestion title to match dataset');
        assert.equal(apiHandler.numberOfCalls, 1, 'expected typeahead endpoint to not be called');

        // debounce
        getFirstSuggestion(component.onTypeahead.perform('hola', DatasetEntity.displayName));
        getFirstSuggestion(component.onTypeahead.perform('hola ', DatasetEntity.displayName));
        const results4 = await getFirstSuggestion(
          component.onTypeahead.perform('hola nacho', DatasetEntity.displayName)
        );
        assert.equal(results4 && results4.title, datasetProps.name, 'expected suggestion title to match dataset');
        assert.equal(apiHandler.numberOfCalls, 2, 'expected requests to typeahead endpoint to be debounced');
      }
    );

    await render(hbs`
      {{#search/containers/search-box  as |keyword entity onSearchInputTask onSearch|}}
        {{container-stub onTypeahead=onSearchInputTask}}
      {{/search/containers/search-box}}
    `);
  });
});
