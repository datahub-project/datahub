import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { buttonClass, State } from '@datahub/shared/components/buttons/binary-toggle-button';
import sinon from 'sinon';

module('Integration | Component | buttons/binary-toggle-button', function(hooks): void {
  const affirmButton = `.${buttonClass}__affirm`;
  const denyButton = `.${buttonClass}__deny`;
  const stateLabels: Record<State, string> = { affirm: 'Affirmed', deny: 'Denied' };

  setupRenderingTest(hooks);

  test('rendering and interaction', async function(assert): Promise<void> {
    const buttonState = State.affirm;
    const onAffirm = sinon.fake();
    const onDeny = sinon.fake();

    await render(hbs`
      <Buttons::BinaryToggleButton
        @state={{this.state}}
        @stateLabels={{this.stateLabels}}
        @onAffirm={{this.onAffirm}}
        @onDeny={{this.onDeny}}
        @affirmDisabled={{this.disableAffirm}}
      />
    `);

    assert.equal(
      document.querySelectorAll(`.${buttonClass}`).length,
      '2',
      'Expected buttons to be rendered when no state applies'
    );

    assert.dom(affirmButton).hasStyle({ color: 'rgb(70, 154, 31)' });
    assert.dom(denyButton).hasStyle({ color: 'rgb(255, 44, 51)' });

    this.setProperties({ state: buttonState, stateLabels });
    assert.dom().hasText(`${stateLabels[buttonState]}`, 'Expected button state to be rendered');
    assert.equal(document.querySelectorAll(`.${buttonClass}`).length, 0, 'Expected buttons to no longer be rendered');

    this.setProperties({ state: undefined, onAffirm, onDeny });

    await click(affirmButton);

    assert.ok(onAffirm.called, 'Expected the onAffirm action to be invoked');

    await click(denyButton);
    assert.ok(onDeny.called, 'Expected the onDeny action to be invoked');

    assert.dom(affirmButton).isNotDisabled('Expected the affirm button to not be disabled');
    this.set('disableAffirm', true);
    assert.dom(affirmButton).isDisabled('Expected the affirm button to be disabled');
  });

  test('interaction when has state', async function(assert): Promise<void> {
    const buttonState = State.affirm;
    const onUndo = sinon.fake();

    this.setProperties({ buttonState, stateLabels, onUndo });

    await render(hbs`
      <Buttons::BinaryToggleButton
        @state={{this.buttonState}}
        @stateLabels={{this.stateLabels}}
        @onUndo={{this.onUndo}}
      />
    `);

    assert.dom().hasText(`${stateLabels[buttonState]}`, 'Expected button state to be rendered');

    await click(`.${buttonClass}__reverse`);
    assert.ok(onUndo.called, 'Expected the external handler for onUndo to be invoked');
  });
});
