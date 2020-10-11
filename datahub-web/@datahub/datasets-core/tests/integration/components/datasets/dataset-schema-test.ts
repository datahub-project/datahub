import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import DatasetSchema from '@datahub/datasets-core/components/datasets/dataset-schema';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { getSerializedMirageModel } from '@datahub/utils/test-helpers/serialize-mirage-model';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';

module('Integration | Component | datasets/dataset-schema', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  let defaultOnError: OnErrorEventHandler;
  // Click helper
  const toggleJson = (): Promise<void> => click('.nacho-toggle__button--right');

  // there is an issue where the worker loading gets cancelled (due to test finished)
  // since we have no control on the Ace lib, we are just ignoring this error as this
  // only affects to syntax validation (if syntax is wrong)
  const ignoreWorkerError = function(this: unknown, _e: unknown, msg: string): void {
    if (msg.indexOf('blob:') === 0) {
      return;
    }

    // eslint-disable-next-line
    defaultOnError?.apply(this, arguments);
  };

  hooks.before(() => {
    defaultOnError = window.onerror;
    window.onerror = ignoreWorkerError;
  });

  hooks.after(() => {
    window.onerror = defaultOnError;
  });

  test('empty component render', async function(assert): Promise<void> {
    await render(hbs`<Datasets::DatasetSchema />`);

    assert.dom().hasText('There was an error retrieving the Schema for this Dataset');
    const schemas: DatasetSchema['args']['schemas'] = [];
    const json = '{}';

    this.setProperties({ json, schemas });

    await render(hbs`
      <Datasets::DatasetSchema
        @schemas={{this.schemas}}
      />
    `);

    assert.dom().hasText('There was an error retrieving the Schema for this Dataset');
  });

  test('component attributes, and interactive JSON rendering', async function(this: MirageTestContext, assert): Promise<
    void
  > {
    const tableSelector = '.dataset-detail-table';
    const json = '{}';

    this.server.createList('datasetSchemaColumns', 10);
    const schemas: DatasetSchema['args']['schemas'] = getSerializedMirageModel('datasetSchemaColumns', this.server);

    this.setProperties({ json, schemas });

    const component = await getRenderedComponent({
      template: hbs`
      <Datasets::DatasetSchema
        @json={{this.json}}
        @schemas={{this.schemas}}
      />
    `,
      ComponentToRender: DatasetSchema,
      componentName: 'datasets/dataset-schema',
      testContext: this
    });

    assert.dom(`${tableSelector} thead`).hasText('Column Data Type Default Comments');
    assert.equal(
      document.querySelectorAll(`${tableSelector} tbody tr`).length,
      schemas.length,
      'Expected a table row to be rendered for each schema column'
    );

    assert
      .dom('.dataset-schema__header')
      .hasText('Table JSON Last modified:', 'Expected toggle header row to be visible');

    await toggleJson();

    assert.notOk(component.isShowingTable, 'Expected the isShowingTable to be false');
    assert.dom(tableSelector).doesNotExist();

    assert.equal(component.aceMode, 'ace/mode/json');
    assert.dom('.dataset-compliance__editor').exists();
    assert.dom('.dataset-compliance__editor .ace_content').hasText('{}');
  });

  test('Non JSON rendering', async function(this: MirageTestContext, assert): Promise<void> {
    const notJson =
      'struct<header__memberid:int,header__viewerurn:string,header__applicationviewerurn:string,header__time:bigint>';

    this.server.createList('datasetSchemaColumns', 1);
    this.setProperties({ notJson, schemas: getSerializedMirageModel('datasetSchemaColumns', this.server) });

    const component = await getRenderedComponent({
      template: hbs`
      <Datasets::DatasetSchema
        @json={{this.notJson}}
        @schemas={{this.schemas}}
      />
    `,
      ComponentToRender: DatasetSchema,
      componentName: 'datasets/dataset-schema',
      testContext: this
    });

    await toggleJson();

    assert.equal(component.aceMode, 'ace/mode/text', 'Expected the mode to change to text when JSON is invalid');
    assert
      .dom('.dataset-compliance__editor .ace_content')
      .hasText(notJson, 'Expected none JSON content to be rendered as text');

    assert.dom('.ace_gutter-cell').hasText('1', 'Expected line numbers to be show in the ace editor');
  });
});
