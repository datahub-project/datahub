import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import LineageLineageContainer from '@datahub/shared/components/lineage/lineage-container';

module('Integration | Component | lineage/lineage-container', function(hooks) {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('it renders', async function(assert) {
    const entityUrn = 'adataset';

    this.set('entityUrn', entityUrn);

    const component = await getRenderedComponent({
      ComponentToRender: LineageLineageContainer,
      componentName: 'lineage/lineage-container',
      template: hbs`
      <Lineage::LineageContainer @urn={{entityUrn}} as |container|>
        template block text
      </Lineage::LineageContainer>
    `,
      testContext: this
    });

    assert.equal(this.element.textContent?.trim(), 'template block text');
    assert.deepEqual(component.state?.graph, { nodes: [], edges: [] });
    assert.ok(component.state?.lineageMode);
  });
});
