import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, waitFor } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { setupErrorHandler } from '@datahub/utils/test-helpers/setup-error';
import { lineageGraph } from '../../../helpers/graph/graphs';

module('Integration | Component | lineage/lineage-main', function(hooks) {
  setupRenderingTest(hooks);
  setupMirage(hooks);
  setupErrorHandler(hooks, { catchMsgs: ['Unexpected token i in JSON at position 2'] });

  test('error case', async function(this: MirageTestContext, assert) {
    this.server.namespace = '/api/v2';
    this.server.get('/lineage/graph/:urn', () => 'failure', 500); // force Mirage to error

    await render(hbs`<Lineage::LineageMain @urn="reallydoesnotmatter"/>`);

    assert.dom('.graph-viewer-toolbar').doesNotExist();
    assert.dom('.empty-state').exists({ count: 1 });
  });

  test('fullscreen', async function(this: MirageTestContext, assert) {
    this.server.get('/lineage/graph/:urn', () => lineageGraph);
    await render(
      hbs`<Lineage::LineageMain @urn="urn:li:dataset:(urn:li:dataPlatform:hdfs,dataset,PROD)" @fullscreenMode={{true}}/>`
    );
    await waitFor('.graph-viewer > svg', { timeout: 10000 });
    assert.dom('.graph-viewer-toolbar').exists();
    assert.dom('.properties-panel__property-name-label').containsText('Datasets');
    assert.dom('.lineage-graph__container--fullscreen').exists();
    assert.dom('.graph-viewer--fullscreen-mode').exists();
  });
});
