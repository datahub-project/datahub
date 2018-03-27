import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';

moduleForComponent('dataset-aclaccess', 'Integration | Component | dataset aclaccess', {
  integration: true
});

test('it renders', function(assert) {
  this.setProperties({
    acls: [],
    accessTypeDropDownOptions: []
  });

  this.render(hbs`{{dataset-aclaccess acls=acls accessTypeDropDownOptions=accessTypeDropDownOptions}}`);

  assert.ok(document.querySelector('.acl-permission__header'), 'it renders a constituent element in the DOM');
});
