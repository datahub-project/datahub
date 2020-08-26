import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll, click, fillIn } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { noop } from 'lodash';

const findInput = (selector: string): HTMLInputElement => {
  const input: HTMLInputElement = find(selector) as HTMLInputElement;

  if (!input) {
    throw new Error(`${selector} element not found`);
  }
  return input;
};

const defaultProperties = {
  updateDeprecation: noop,
  entityName: 'dataset',
  deprecationNoteAlias: 'type something',
  deprecated: false
};

module('Integration | Component | entity-page/entity-deprecation', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    assert.expect(5);
    this.setProperties(defaultProperties);

    await render(hbs`{{entity-page/entity-deprecation
                       updateDeprecation=updateDeprecation
                       entityName=entityName}}`);

    assert.ok(this, 'Tab renders in most default state without errors');
    assert.ok(find('.entity-deprecation__toggle-header__label'), 'it shows the entity deprecation label element');
    assert.equal(findAll('#dataset-is-deprecated').length, 1, 'has one input checkbox with known selector');
    assert.equal(
      findInput('#dataset-is-deprecated').getAttribute('type'),
      'checkbox',
      'has an input checkbox to toggle deprecation'
    );
    assert.equal(findAll('.entity-deprecation__actions').length, 1, 'has an actions container');
  });

  test('setting the deprecated property should toggle the checkbox', async function(assert): Promise<void> {
    assert.expect(2);

    this.setProperties({
      ...defaultProperties,
      deprecated: true
    });

    await render(hbs`{{entity-page/entity-deprecation
                       updateDeprecation=updateDeprecation
                       entityName=entityName
                       isDeprecated=deprecated}}`);

    assert.ok(
      (find('#dataset-is-deprecated') as HTMLInputElement).checked,
      'checkbox is checked when property is set true'
    );
    this.set('deprecated', false);
    assert.notOk(
      (find('#dataset-is-deprecated') as HTMLInputElement).checked,
      'checkbox is unchecked when property is set false'
    );
  });

  test('decommissionTime', async function(assert): Promise<void> {
    let isDisabled;
    assert.expect(3);

    this.setProperties({
      ...defaultProperties,
      decommissionTime: void 0,
      deprecated: true
    });

    await render(hbs`{{entity-page/entity-deprecation
                       entityName=entityName
                       isDeprecated=deprecated
                       decommissionTime=decommissionTime
                       updateDeprecation=(action updateDeprecation)}}`);

    isDisabled = findInput('.entity-deprecation__actions [type=submit]').disabled;
    assert.ok(isDisabled, 'submit button is disabled');

    this.setProperties({ decommissionTime: new Date(), isDirty: true });
    await render(hbs`{{entity-page/entity-deprecation
                       entityName=entityName
                       isDeprecated=deprecated
                       decommissionTime=decommissionTime
                       updateDeprecation=(action updateDeprecation)}}`);

    await fillIn('.entity-deprecation__note-editor .medium-editor-element', 'text');

    isDisabled = findInput('.entity-deprecation__actions [type=submit]').disabled;
    assert.ok(isDisabled, 'submit button is disabled if we only fill in decomissionTime');

    await click('#acknowledge-deprecation');

    isDisabled = findInput('.entity-deprecation__actions [type=submit]').disabled;
    assert.notOk(isDisabled, 'submit button is disabled if we only fill in decomissionTime');
  });

  test('triggers the onUpdateDeprecation action when submitted', async function(assert): Promise<void> {
    let submitActionCallCount = 0;

    this.setProperties({
      ...defaultProperties,
      submit(deprecated: boolean, note: string): void {
        submitActionCallCount++;
        assert.equal(deprecated, true, 'action is called with deprecation value of true');
        assert.equal(note, '', 'action is called with an empty deprecation note');
      },
      decommissionTime: new Date()
    });

    await render(hbs`{{entity-page/entity-deprecation
                       entityName=entityName
                       isDeprecated=deprecated
                       decommissionTime=decommissionTime
                       updateDeprecation=(action submit)}}`);

    assert.equal(submitActionCallCount, 0, 'action is not called on render');
    assert.equal(
      (find('#dataset-is-deprecated') as HTMLInputElement).checked,
      false,
      'deprecation checkbox is unchecked'
    );

    await click('#dataset-is-deprecated');

    assert.equal((find('#dataset-is-deprecated') as HTMLInputElement).checked, true, 'deprecation checkbox is checked');

    await click('#acknowledge-deprecation');
    await click('.entity-deprecation__actions [type=submit]');

    assert.equal(submitActionCallCount, 1, 'action is called once');
  });
});
