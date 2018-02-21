import { moduleForComponent, test } from 'ember-qunit';
import { click } from 'ember-native-dom-helpers';
import notificationsStub from 'wherehows-web/tests/stubs/services/notifications';
import hbs from 'htmlbars-inline-precompile';
import { run } from '@ember/runloop';

moduleForComponent('dataset-deprecation', 'Integration | Component | dataset deprecation', {
  integration: true
});

test('it renders', function(assert) {
  assert.expect(4);

  this.render(hbs`{{dataset-deprecation}}`);

  assert.equal(
    this.$()
      .text()
      .trim(),
    'Is this dataset deprecated?',
    'shows the question asking if the dataset is deprecated'
  );
  assert.equal(this.$('#dataset-is-deprecated').length, 1, 'has one input checkbox with known selector');
  assert.equal(
    this.$('#dataset-is-deprecated').attr('type'),
    'checkbox',
    'has an input checkbox to toggle deprecation'
  );
  assert.equal(this.$('.dataset-deprecation-toggle__actions').length, 1, 'has an actions container');
});

test('setting the deprecated property should toggle the checkbox', function(assert) {
  assert.expect(2);

  this.set('deprecated', true);

  this.render(hbs`{{dataset-deprecation deprecated=deprecated}}`);

  assert.ok(this.$('#dataset-is-deprecated').is(':checked'), 'checkbox is checked when property is set true');

  this.set('deprecated', false);
  assert.notOk(this.$('#dataset-is-deprecated').is(':checked'), 'checkbox is unchecked when property is set false');
});

test('triggers the onUpdateDeprecation action when submitted', async function(assert) {
  let submitActionCallCount = 0;

  this.set('submit', function(deprecated, note) {
    submitActionCallCount++;
    assert.equal(deprecated, true, 'action is called with deprecation value of true');
    assert.equal(note, null, 'action is called with null deprecation note');
  });

  this.render(hbs`{{dataset-deprecation onUpdateDeprecation=(action submit)}}`);

  assert.equal(submitActionCallCount, 0, 'action is not called on render');
  assert.equal(this.$('#dataset-is-deprecated').is(':checked'), false, 'deprecation checkbox is unchecked');

  await click('#dataset-is-deprecated');

  assert.equal(this.$('#dataset-is-deprecated').is(':checked'), true, 'deprecation checkbox is checked');
  await click('.dataset-deprecation-toggle__actions [type=submit]');

  assert.equal(submitActionCallCount, 1, 'action is called once');
});
