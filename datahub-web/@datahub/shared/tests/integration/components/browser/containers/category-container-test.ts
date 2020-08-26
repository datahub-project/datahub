import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import BrowserContainersCategoryContainer from '@datahub/shared/components/browser/containers/category-container';
import CurrentUser from '@datahub/shared/services/current-user';
import DataModelsService from '@datahub/data-models/services/data-models';
import sinonTest from 'ember-sinon-qunit/test-support/test';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

const id = 'browser-container';

module('Integration | Component | browser/containers/category-container', function(hooks): void {
  setupRenderingTest(hooks);
  hooks.beforeEach(function() {
    stubService('configurator', {
      getConfig(): boolean {
        return false;
      }
    });
  });
  test('it renders and has expected properties', async function(assert): Promise<void> {
    const entities = [{}];

    this.setProperties({ entities, id });

    const component = await getRenderedComponent({
      testContext: this,
      ComponentToRender: BrowserContainersCategoryContainer,
      template: hbs`<Browser::Containers::CategoryContainer @entities={{this.entities}} id={{this.id}} />`,
      componentName: 'browser/containers/category-container'
    });

    assert.ok(
      component.sessionUser instanceof CurrentUser,
      'Expected the BrowserContainersCategoryContainer to have  the currentUser service injected for related composite tracking tasks'
    );

    assert.dom(`#${id}`).hasText('Browse Types of Entities');
  });

  sinonTest('nested click events bubble to container and container invokes track event handler', async function(
    this: SinonTestContext,
    assert
  ): Promise<void> {
    const dataModelsService: DataModelsService = this.owner.lookup('service:data-models');
    const service: UnifiedTracking = this.owner.lookup('service:unified-tracking');
    const stubbedTrackEvent = this.stub(service, 'trackEvent');

    const entities = [
      {
        title: 'Browse Card',
        text: 'A browse card',
        route: '',
        model: dataModelsService.getModel('datasets').displayName
      }
    ];

    this.setProperties({ entities, id });

    await render(hbs`
      <Browser::Containers::CategoryContainer @entities={{this.entities}} id={{this.id}} />
      `);

    await click('.browse-card');

    assert.ok(stubbedTrackEvent.called, "Expected the UnifiedTracking service's trackEvent method to have been called");
    assert.dom(`#${id}`).hasText('Browse Types of Entities Browse Card A browse card');
  });
});
