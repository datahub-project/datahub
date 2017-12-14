import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

import { Classification } from 'wherehows-web/constants';
import { triggerEvent } from 'ember-native-dom-helpers';

moduleForComponent('datasets/schemaless-tagging', 'Integration | Component | datasets/schemaless tagging', {
  integration: true
});

test('it renders', function(assert) {
  assert.expect(2);
  const elementId = 'test-schemaless-component-1337';
  this.set('elementId', elementId);
  this.render(hbs`{{datasets/schemaless-tagging elementId=elementId}}`);

  assert.ok(document.querySelector(`#${elementId}-schemaless-checkbox`), 'it renders a checkbox component');
  assert.ok(document.querySelector(`#${elementId} select`), 'it renders a select drop down');
});

test('it shows the current classification', function(assert) {
  assert.expect(3);
  this.render(hbs`{{datasets/schemaless-tagging classification=classification}}`);

  assert.equal(document.querySelector(`select`).value, 'Unspecified', "displays 'Unspecified' when not set");

  this.set('classification', Classification.LimitedDistribution);

  assert.equal(
    document.querySelector(`select`).value,
    Classification.LimitedDistribution,
    `displays ${Classification.LimitedDistribution} when set`
  );

  this.set('classification', Classification.Confidential);

  assert.equal(
    document.querySelector('select').value,
    Classification.Confidential,
    `displays ${Classification.Confidential} when changed`
  );
});

test('it correctly indicates if the dataset has pii', function(assert) {
  assert.expect(2);
  this.set('containsPersonalData', true);

  this.render(hbs`{{datasets/schemaless-tagging containsPersonalData=containsPersonalData}}`);

  assert.equal(document.querySelector('.toggle-switch').checked, true, 'checkbox is checked when true');

  this.set('containsPersonalData', false);

  assert.notOk(document.querySelector('.toggle-switch').checked, 'checkbox is unchecked when false');
});

test('it invokes the onClassificationChange external action when change is triggered', function(assert) {
  assert.expect(2);
  let onClassificationChangeCallCount = 0;

  this.set('isEditable', true);
  this.set('classification', Classification.LimitedDistribution);
  this.set('onClassificationChange', () => {
    assert.equal(++onClassificationChangeCallCount, 1, 'successfully invokes the external action');
  });

  this.render(
    hbs`{{datasets/schemaless-tagging isEditable=isEditable onClassificationChange=onClassificationChange classification=classification}}`
  );

  assert.equal(onClassificationChangeCallCount, 0, 'external action is not invoked on instantiation');

  triggerEvent('select', 'change');
});

test('it invokes the onPersonalDataChange external action on when toggled', function(assert) {
  assert.expect(3);

  let onPersonalDataChangeCallCount = 0;

  this.set('isEditable', true);
  this.set('containsPersonalData', false);
  this.set('onPersonalDataChange', containsPersonalData => {
    assert.equal(++onPersonalDataChangeCallCount, 1, 'successfully invokes the external action');
    assert.ok(containsPersonalData, 'flag value is truthy');
  });

  this.render(
    hbs`{{datasets/schemaless-tagging isEditable=isEditable onPersonalDataChange=onPersonalDataChange containsPersonalData=containsPersonalData}}`
  );

  assert.equal(onPersonalDataChangeCallCount, 0, 'external action is not invoked on instantiation');
  triggerEvent('[type=checkbox]', 'click');
});
