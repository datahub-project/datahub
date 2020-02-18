import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { ISearchDataWithMetadata } from '@datahub/data-models/types/entity/search';

module('Integration | Component | search/search-result', function(hooks) {
  setupRenderingTest(hooks);

  interface IMockEntity {
    urn: string;
    name: string;
  }

  const createEntity = (): ISearchDataWithMetadata<IMockEntity> => ({
    data: {
      urn: 'test_urn',
      name: 'testResult'
    },
    meta: {
      entityLink: {
        entity: DatasetEntity.displayName,
        link: {
          model: ['datasets.dataset'],
          queryParams: {},
          route: 'datasets.dataset',
          text: 'testResult',
          title: 'testResult'
        }
      },
      resultPosition: 1,
      instance: null
    }
  });

  test('it renders', async function(assert): Promise<void> {
    assert.expect(1);

    const result = createEntity();

    this.setProperties({ searchConfig: { attributes: [] }, result });
    await render(hbs`<Search::SearchResult
      @result={{this.result.data}}
      @meta={{this.result.meta}}
      @searchConfig={{this.searchConfig}}
    />`);

    assert.ok(find('.search-result'), 'expected component to have a class `search-result`');
  });

  test('search-result properties', async function(assert): Promise<void> {
    assert.expect(1);

    const result = createEntity();

    this.setProperties({ searchConfig: { attributes: [] }, result });
    await render(hbs`<Search::SearchResult
      @result={{this.result.data}}
      @meta={{this.result.meta}}
      @searchConfig={{this.searchConfig}}
    />`);

    const searchResultElement: Element | null = find('.search-result');
    const title = searchResultElement && searchResultElement.querySelector('.search-result__title');
    const titleTextContentIncludes = title && title.textContent && title.textContent.trim().includes(result.data.name);

    assert.ok(titleTextContentIncludes, 'expected result name to be rendered in title element');
  });
});
