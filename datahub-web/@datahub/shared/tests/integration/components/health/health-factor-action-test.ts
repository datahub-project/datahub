import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, settled, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import sinon from 'sinon';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { TrackingEventCategory } from '@datahub/shared/constants/tracking/event-tracking';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import HealthHealthFactorAction from '@datahub/shared/components/health/health-factor-action';

module('Integration | Component | health/health-factor-action', function(hooks): void {
  setupRenderingTest(hooks);

  const rowData = { validator: 'Ownership' };
  const labelConfig = { componentAction: sinon.fake() };
  const tableConfigs = { options: { wikiLink: 'http://example.com' } };

  test('tracking interactions', async function(assert): Promise<void> {
    const category = TrackingEventCategory.ControlInteraction;
    const ownershipAction = 'DataHubHealthScoreFactorsActionViewOwnership';
    const descriptionAction = 'DataHubHealthScoreFactorsActionViewDescription';
    const stubbedTrackEvent = sinon.fake();
    stubService('unified-tracking', {
      trackEvent: stubbedTrackEvent
    });

    this.setProperties({ rowData, labelConfig });

    const component = await getRenderedComponent({
      template: hbs`<Health::HealthFactorAction
        @rowData={{this.rowData}}
        @labelConfig={{this.labelConfig}}
        @tableConfigs={{this.tableConfigs}}
      />`,
      ComponentToRender: HealthHealthFactorAction,
      testContext: this,
      componentName: 'health/health-factor-action'
    });

    await click('button');
    assert.ok(
      stubbedTrackEvent.calledWith({ category, action: ownershipAction }),
      'Expected tracking handler, trackEvent to have been called with ownership action and category'
    );
    this.set('rowData', { ...rowData, validator: 'Description' });

    await settled();

    assert.equal(component.controlName, descriptionAction, 'Expected control name to match description control name');
  });

  test('it renders', async function(assert): Promise<void> {
    const actionLinkSelector = '.health-factor-action__wiki';

    await render(hbs`<Health::HealthFactorAction
      @rowData={{this.rowData}}
      @labelConfig={{this.labelConfig}}
      @tableConfigs={{this.tableConfigs}}
    />`);

    assert.dom().hasText('', 'Expected DOM to not contain any text when no CTA is provided');
    assert.dom('td.nacho-table__cell').exists();
    assert.dom('button').doesNotExist();

    this.setProperties({ rowData, labelConfig });
    await settled();

    assert.dom().hasText('View Owners', 'Expected cta to be shown');

    await click('button');
    assert.ok(
      labelConfig.componentAction.calledWith(rowData),
      'Expected the component action to be called with rowData on click'
    );

    this.setProperties({ rowData: { validator: 'Description' }, tableConfigs });
    await settled();

    assert.dom(actionLinkSelector).hasText('Wiki', 'Expected wiki link cta to be shown and link to exist');
    assert
      .dom(actionLinkSelector)
      .hasAttribute(
        'href',
        tableConfigs.options.wikiLink,
        'Expected the hyperlink reference to match the config value'
      );
  });
});
