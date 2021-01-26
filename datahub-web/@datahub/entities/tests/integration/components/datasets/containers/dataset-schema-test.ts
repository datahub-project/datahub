import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { urn } from '@datahub/shared/mirage-addon/test-helpers/urn';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';

module('Integration | Component | datasets/containers/dataset-schema', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('renders default state as expected', async function(assert): Promise<void> {
    this.set('urn', urn);

    await render(hbs`<Datasets::Containers::DatasetSchema @entity={{hash urn=this.urn}} />`);
    assert
      .dom('.empty-state__header')
      .hasText('We could not find a schema for this dataset', 'renders empty State with an error prompt');
  });
});
