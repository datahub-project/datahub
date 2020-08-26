import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import HealthEntityDetail, { baseClass } from '@datahub/shared/components/health/entity-detail';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

module('Integration | Component | health/entity-detail', function(hooks): void {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    stubService('configurator', {
      getConfig(): object {
        return {
          wikiLinks: {
            metadataHealth: 'http://www.example.com'
          }
        };
      }
    });
  });

  test('base rendering', async function(assert): Promise<void> {
    await render(hbs`<Health::EntityDetail />`);

    assert
      .dom()
      .hasText(
        'Health Factors How is the health score computed? Overall Health: N/A Recalculate Score Factor Score Impact on Health Score Description'
      );
  });

  test('component attributes and wiki render', async function(assert): Promise<void> {
    const wiki = {
      metadataHealth: 'http://www.example.com'
    };

    stubService('configurator', {
      getConfig(): object {
        return wiki;
      }
    });
    const headerSelector = `.${baseClass}__header`;

    this.set('wiki', wiki);

    const component = await getRenderedComponent({
      template: hbs`<Health::EntityDetail @wikiLinks={{this.wiki}} />`,
      ComponentToRender: HealthEntityDetail,
      testContext: this,
      componentName: 'health/entity-detail'
    });

    assert.equal(
      component.healthFactorsWiki,
      wiki.metadataHealth,
      'Expected the component to have a reference to Health Factors'
    );

    assert.dom(`${headerSelector} .more-info`).exists('Expected the tooltip for health wiki to be present');
    assert.dom(`${headerSelector} a`).hasAttribute('href', wiki.metadataHealth);
  });
});
