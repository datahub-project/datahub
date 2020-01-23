import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import { getText } from '@datahub/utils/test-helpers/dom-helpers';
import hbs from 'htmlbars-inline-precompile';
import { TestContext } from 'ember-test-helpers';
import { setProperties } from '@ember/object';
import { IFacetsSelectionsMap, IFacetsCounts } from '@datahub/data-models/types/entity/facets';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';

interface ITestWithMirageContext extends TestContext {
  server: any;
  fields: Array<ISearchEntityRenderProps>;
  selections: IFacetsSelectionsMap;
  counts: IFacetsCounts;
  onFacetsChange: () => void;
}

module('Integration | Component | search/containers/search-facets', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(this: ITestWithMirageContext, assert) {
    setProperties(this, {
      fields: DatasetEntity.renderProps.search.attributes,
      counts: {
        origin: {
          PROD: 1,
          CORP: 10
        }
      },
      selections: {},
      onFacetsChange: () => {}
    });
    await render(hbs`
      {{#search/containers/search-facets
        fields=fields
        counts=counts
        selections=selections
        onFacetsChange=(action onFacetsChange)
      as |searchFacet selections counts onFacetChange onFacetClear|}}
        {{#each searchFacet as |facet|}}
          {{facet.name}}-{{facet.values.length}}
          {{#each facet.values as |value|}}
            {{value.label}}
          {{/each}}
        {{/each}}
      {{/search/containers/search-facets}}
    `);

    assert.equal(
      getText(this)
        .replace(/\s/gi, '')
        .trim(),
      'origin-2CORPPROD',
      'Should show just origin but not platform and sorted'
    );
  });
});
