import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, settled, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import healthScenario from '@datahub/shared/mirage-addon/scenarios/health-metadata';
import { baseClass } from '@datahub/shared/components/health/insight-card';
import RouterService from '@ember/routing/router-service';
import sinonTest from 'ember-sinon-qunit/test-support/test';
import { SinonStub } from 'sinon';

module('Integration | Component | health/carousel-insight', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('rendering without attributes', async function(assert): Promise<void> {
    stubService('configurator', {
      getConfig(): object {
        return {};
      }
    });

    await render(hbs`
      <Health::CarouselInsight />
    `);

    assert
      .dom()
      .hasText(
        'Health No health score available Recalculate Score',
        'Expected message indicating no health data to be shown'
      );
    assert.dom('.more-info').doesNotExist();
    assert
      .dom('[data-insight-action="recalculate"]')
      .hasTagName(
        'button',
        'Expected the recalculate action to be rendered as as button when there is no Health Score available'
      );
  });

  sinonTest('rendering with attributes', async function(
    this: SinonTestContext & MirageTestContext,
    assert
  ): Promise<void> {
    healthScenario(this.server);

    const entity = new MockEntity('test-urn');
    const recalculationButtonSelector = '[data-insight-action="recalculate"]';
    const viewButtonSelector = '[data-insight-action="view"]';
    const menuSelector = '.nacho-drop-down';
    const scoreClassSelector = `.${baseClass}__score-value`;
    const wikiLinks = {
      metadataHealth: 'http://www.example.com'
    };

    stubService('configurator', {
      getConfig(): object {
        return wikiLinks;
      }
    });

    stubService('data-models', {
      getModel(): typeof MockEntity {
        return MockEntity;
      }
    });

    stubService('router', {
      isActive() {
        return false;
      }
    });
    const service: RouterService = this.owner.lookup('service:router');
    let stubbedIsActive = this.stub(service, 'isActive').callsFake(() => false);

    this.setProperties({ options: {}, entity });

    await render(hbs`
      <Health::CarouselInsight @options={{this.options}} @entity={{this.entity}} />
    `);

    assert.dom(scoreClassSelector).hasText(/\d{1,3}\%/, 'Expected message indicating no health data to be shown');
    assert.dom(viewButtonSelector).hasText('See Details', 'Expected view button to exist');
    assert.dom(menuSelector).exists('Expected the menu to exist');
    await triggerEvent('.health-insight-card__menu-trigger', 'mouseenter');
    assert
      .dom('.nacho-drop-down__options')
      .hasText('Recalculate Score', 'Expected the menu option to show the option to recalculate the health score');

    assert
      .dom(recalculationButtonSelector)
      .doesNotExist('Expected the recalculation button to not be rendered when the view button is visible');

    assert.dom('.more-info a').hasAttribute('href', wikiLinks.metadataHealth);

    assert.ok(stubbedIsActive.called, 'Expected is active to be called on component render');

    (service.isActive as SinonStub).restore();
    // Make the route active
    stubbedIsActive = this.stub(service, 'isActive').callsFake(() => true);
    // Trigger a change on the isViewingHealth dependent key
    service.set('currentURL', 'newCurrentUrl');
    await settled();

    assert.dom(viewButtonSelector).doesNotExist('Expected the view button to not be rendered when the route is active');
    assert
      .dom(recalculationButtonSelector)
      .exists('Expected the recalculation button to be rendered when the route is active');
    assert.dom(menuSelector).doesNotExist('Expected the insight menu to not exist when viewing the health tab');
  });
});
