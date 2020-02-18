import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const findInput = (selector: string): HTMLInputElement => {
  const input: HTMLInputElement = find(selector) as HTMLInputElement;

  if (!input) {
    throw new Error(`${selector} element not found`);
  }
  return input;
};

const defaultProperties = {
  updateDeprecation(): void {},
  entityName: 'dataset',
  deprecationNoteAlias: 'type something',
  deprecated: false
};

module('Integration | Component | entity-deprecation', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    assert.expect(5);
    this.setProperties(defaultProperties);

    await render(hbs`{{entity-deprecation
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

    await render(hbs`{{entity-deprecation
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

    this.setProperties({
      ...defaultProperties,
      decommissionTime: void 0,
      deprecated: true
    });

    await render(hbs`{{entity-deprecation
                       entityName=entityName
                       isDeprecated=deprecated
                       decommissionTime=decommissionTime
                       updateDeprecation=(action updateDeprecation)}}`);

    isDisabled = findInput('.entity-deprecation__actions [type=submit]').disabled;
    assert.ok(isDisabled, 'submit button is disabled');
  });
});
