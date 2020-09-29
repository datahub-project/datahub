import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, fillIn } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { waitFor, click } from '@ember/test-helpers';
import { simpleGraph, lineageGraph } from '../../../helpers/graph/graphs';
import { typeInSearch } from 'ember-power-select/test-support/helpers';
import { IGraphViewerState } from '@datahub/shared/types/graph/graph-viewer-state';

module('Integration | Component | graph-viewer & toolbar', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    let state: IGraphViewerState = {
      graph: simpleGraph
    };
    let lastState: IGraphViewerState | undefined;

    const onStateChanged = (newState: IGraphViewerState): void => {
      lastState = state;
      state = newState;
      this.setProperties({
        state,
        lastState
      });
    };
    this.setProperties({ state, lastState, onStateChanged });

    await render(
      hbs`
        <Graph::Toolbar @state={{this.state}} @lastState={{this.lastState}} @onStateChanged={{this.onStateChanged}} />
        <Graph::GraphViewer @state={{this.state}} @lastState={{this.lastState}} @onStateChanged={{this.onStateChanged}}/>
      `
    );

    // wait for worker to paint the svg
    await waitFor('.graph-viewer svg', { timeout: 1000 });
    await waitFor('.graph-viewer__title', { timeout: 1000 });

    assert.dom('.graph-viewer__title').containsText('displayName1');
    assert.dom('.graph-viewer__property-name').containsText('reference');
    assert.dom('.graph-viewer__property-type').containsText('DisplayName2');
    assert.dom().doesNotContainText('attribute');
    assert.dom('[id="ENTITY::id1"]').hasClass('selected');

    // click another entity, expect selected
    await click('[id="ENTITY-TITLE::id2"]');

    assert.dom('[id="ENTITY::id1"]').doesNotHaveClass('selected');
    assert.dom('[id="ENTITY::id2"]').hasClass('selected');

    // Search 'dis' (multiple results)
    await click('.graph-viewer-toolbar__toolbar-button-search');
    await typeInSearch('dis');

    await click('.ember-power-select-option:first-child');

    assert.dom('[id="ENTITY::id1"]').hasClass('selected');
    assert.dom('[id="ENTITY::id2"]').doesNotHaveClass('selected');

    // Search 'id2' (id search)
    await click('.graph-viewer-toolbar__toolbar-button-search');
    await typeInSearch('id2');

    await click('.ember-power-select-option:first-child');

    assert.dom('[id="ENTITY::id1"]').doesNotHaveClass('selected');
    assert.dom('[id="ENTITY::id2"]').hasClass('selected');

    // Search 'displayName1' (display name search)
    await click('.graph-viewer-toolbar__toolbar-button-search');
    await typeInSearch('displayName1');

    await click('.graph-viewer-toolbar__toolbar-button-configuration');

    await click('#show-connections-only');
    await click('.graph-viewer-toolbar__toolbar-submit-button');
    await waitFor('.graph-viewer__property-name', { timeout: 1000 });

    assert.dom().containsText('attribute');

    assert.dom('.graph-viewer-toolbar__lineage-depth-info').doesNotExist();
  });

  test('it renders lineage', async function(assert): Promise<void> {
    let state: IGraphViewerState = {
      graph: lineageGraph,
      lineageMode: true,
      lineageDepth: 5
    };
    let lastState: IGraphViewerState | undefined;

    const onStateChanged = (newState: IGraphViewerState): void => {
      lastState = state;
      state = newState;
      this.setProperties({
        state,
        lastState
      });
    };
    this.setProperties({ state, lastState, onStateChanged });

    await render(
      hbs`
        <Graph::Toolbar @state={{this.state}} @lastState={{this.lastState}} @onStateChanged={{this.onStateChanged}} />
        <Graph::GraphViewer @state={{this.state}} @lastState={{this.lastState}} @onStateChanged={{this.onStateChanged}}/>
      `
    );

    // wait for worker to paint the svg
    await waitFor('.graph-viewer svg', { timeout: 1000 });
    await waitFor('.graph-viewer__title', { timeout: 1000 });

    assert.dom('[id="ENTITY-ACTION-GO-TO-ENTITY::3"]').exists();
    assert.dom('[id="ENTITY-ACTION-GO-TO-ENTITY::4"]').doesNotExist();

    await click('[id="ENTITY-TITLE::5"]');

    assert.dom('[id="ENTITY::4"]').hasClass('node-related');
    assert.dom('[id="ENTITY::6"]').hasClass('node-related');
    assert.dom('[id="ENTITY::5"]').hasClass('selected');
    assert.dom('[id="EDGE::3::root::4"]').hasClass('edge-from-selected');
    assert.dom('[id="EDGE::4::root::5"]').hasClass('edge-from-selected');
    assert.dom('[id="EDGE::4::root::6"]').hasClass('edge-from-selected');
    assert.dom('[id="EDGE::6::root::5"]').hasClass('edge-from-selected');

    await click('[id="ENTITY-TITLE::0"]');
    assert.dom('[id="ENTITY::0"]').hasClass('selected');
    assert.dom('[id="ENTITY::2"]').hasClass('node-related');
    assert.dom('[id="EDGE::0::root::2"]').hasClass('edge-to-selected');
    assert.dom('[id="EDGE::2::root::3"]').hasClass('edge-to-selected');
    assert.dom('[id="EDGE::0::root::3"]').hasClass('edge-to-selected');

    assert.dom('.graph-viewer-toolbar__lineage-depth-info').exists();

    await click('.graph-viewer-toolbar__toolbar-button-configuration');

    assert.dom('.graph-viewer-toolbar__toolbar-input-number').exists();
    assert.dom('.graph-viewer-toolbar__toolbar-input-number').hasValue('5');

    await fillIn('.graph-viewer-toolbar__toolbar-input-textarea', '3');
    await click('.graph-viewer-toolbar__toolbar-submit-button');
    await waitFor('.graph-viewer svg', { timeout: 1000 });

    assert.dom('.graph-viewer-toolbar__toolbar-item').hasText('Hiding 7 nodes');

    await click('.graph-viewer-toolbar__toolbar-button-configuration');
    await fillIn('.graph-viewer-toolbar__toolbar-input-number', '1');
    await click('.graph-viewer-toolbar__toolbar-submit-button');
    await waitFor('.graph-viewer svg', { timeout: 1000 });
    assert.dom('.graph-viewer-toolbar__lineage-depth-info').hasText('Showing 1 levels of upstream and downstream');
    assert.equal(state.lineageDepth, 1);
  });
});
