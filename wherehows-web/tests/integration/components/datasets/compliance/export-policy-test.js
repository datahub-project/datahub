import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, click, waitFor } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const header = '.metadata-prompt__header__text';
const table = '.nacho-table.nacho-table--bordered.nacho-table--stripped';
const editButton = '.edit-button';
const actionBar = 'section.action-bar';
const UGCRadioButton = 'input[name="containsUserGeneratedContent"][value="true"]';
const saveButton = 'button.action-bar__item:nth-child(1)';

module('Integration | Component | datasets/compliance/export-policy', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{datasets/compliance/export-policy}}`);
    assert.ok(this.element, 'Renders independently with no errors');

    this.setProperties({
      exportPolicy: {
        containsUserGeneratedContent: false,
        containsUserActionGeneratedContent: false,
        containsUserDerivedContent: false
      },
      isEditing: false
    });

    await render(hbs`{{datasets/compliance/export-policy
                       exportPolicy=exportPolicy
                       isEditing=isEditing}}`);
    assert.ok(this.element, 'Also renders without errors when passed expected properties');
    assert.ok(find(header).textContent.includes('Data Export Metadata Annotations'), 'Title includes correct text');
    assert.ok(find(table), 'Renders a table');
  });

  test('it functions as expected', async function(assert) {
    this.setProperties({
      exportPolicy: {
        containsUserGeneratedContent: false,
        containsUserActionGeneratedContent: false,
        containsUserDerivedContent: false
      },
      isEditing: false,
      toggleEditing: editingMode => this.set('isEditing', editingMode),
      onSaveExportPolicy: exportPolicy => {
        assert.equal(exportPolicy.containsUserGeneratedContent, true, 'Sends the correct export policy object');
      }
    });

    await render(hbs`{{datasets/compliance/export-policy
                       exportPolicy=exportPolicy
                       isEditing=isEditing
                       toggleEditing=toggleEditing
                       onSaveExportPolicy=onSaveExportPolicy}}`);

    assert.expect(7);
    assert.ok(this.element, 'Still renders without errors');
    assert.equal(this.get('isEditing'), false, 'Base case for is editing test');
    assert.notOk(find(actionBar), 'Action bar does not exist without editing mode');

    await click(editButton);

    assert.equal(this.get('isEditing'), true, 'Edit button works');
    assert.ok(find(actionBar), 'Action bar now exists with editing mode');
    assert.ok(find(`${UGCRadioButton}`), 'Edit radio butotn for UGC exists');

    await click(`${UGCRadioButton}`);
    await click(saveButton);
  });
});
