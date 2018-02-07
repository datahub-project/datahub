import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';
import {
  accessInfoTesting as accessInfo,
  approvedResponseTesting as approvedResponse
} from 'wherehows-web/mirage/helpers/aclauth';

moduleForComponent('dataset-aclaccess', 'Integration | Component | dataset aclaccess', {
  integration: true
});

//TODO: Define testing case
const currentUser = 'Mitchell_Rath';
const accessResponse = { isApproved: false };

//TODO: test each part of the component loading situation
test('it renders', function(assert) {
  this.setProperties({
    accessInfo: accessInfo(true),
    currentUser
  });

  this.render(hbs`{{dataset-aclaccess accessInfo=accessInfo currentUser=currentUser}}`);

  assert.ok(this.$(), 'Render without errors');

  assert.ok(this.$('#acl-permissioninfo'), 'Render permission info section without errors');

  assert.ok(this.$('#acl-permissionrequest'), 'Render permission request section without errors');

  assert.ok(this.$('#acl-accessusers'), 'Render access users section without errors');
});

test('component content renders correctly with permission', function(assert) {
  this.setProperties({
    accessInfo: accessInfo(true),
    currentUser
  });

  this.render(hbs`{{dataset-aclaccess accessInfo=accessInfo currentUser=currentUser}}`);

  assert.ok(
    this.$('i[title="permission"]').attr('class', 'fa fa-check-circle-o fa-lg acl-permission__success'),
    'Render the icon is correctly with the permission'
  );

  assert.equal(
    this.$('.acl-permission__meta')
      .text()
      .trim(),
    'Mitchell_Rath, you have access to this data',
    'Render the permission message'
  );

  assert.equal(this.$('.acl-table__header').text(), 'People with ACL Access', 'Render users list title correctly');

  //table has 7 colums
  assert.equal(this.$('td').length, 7, 'Render table header correctly');
});

test('component content renders correctly without permission', function(assert) {
  this.setProperties({
    accessInfo: accessInfo(false),
    currentUser
  });

  this.render(hbs`{{dataset-aclaccess accessInfo=accessInfo currentUser=currentUser}}`);

  assert.ok(
    this.$('i[title="permission"]').attr('class', 'fa fa-ban fa-lg acl-permission__reject'),
    'Render icon is correctly in permission info section'
  );

  assert.ok(this.$('.acl-form'), 'Render permission request section without errors');

  assert.equal(
    this.$('.acl-permission__meta')
      .text()
      .trim(),
    'Mitchell_Rath, you currently do not have access to this dataset',
    'Render permission message correctly'
  );

  assert.equal(
    this.$('.acl-form__meta.acl-form__header')
      .text()
      .trim(),
    'Request Access',
    'Render request form title correctly'
  );

  assert.equal(
    this.$('.acl-form__meta__header')
      .text()
      .trim(),
    'Why do you need access?',
    'Render request form subtitle 1 correctly'
  );

  assert.equal(
    this.$('.acl-form__meta__content')
      .text()
      .trim(),
    'Please add business reason you need to access this data.',
    'Render request form subtitle 2 correctly'
  );
});

test('component content renders permission rejected', function(assert) {
  this.setProperties({
    accessInfo: accessInfo(false),
    currentUser,
    accessResponse
  });

  this.render(hbs`
  {{dataset-aclaccess
    accessInfo=accessInfo
    currentUser=currentUser
    accessResponse=accessResponse}}`);

  assert.ok(
    this.$('i[title="permission"]').attr('class', 'acl-permission__reject'),
    'Render icon is correctly in permission info section'
  );

  assert.equal(
    this.$('.acl-permission__meta')
      .children('p')
      .text()
      .trim(),
    'If you feel this is in error, contact acreqjests@linkedin.',
    'Render request form subtitle 2 correctly'
  );
});

test('component content renders permission approved', function(assert) {
  this.setProperties({
    accessInfo: accessInfo(false),
    currentUser,
    accessResponse: approvedResponse
  });

  this.render(hbs`
  {{dataset-aclaccess
    accessInfo=accessInfo
    currentUser=currentUser
    accessResponse=accessResponse}}`);

  assert.ok(
    this.$('i[title="permission"]').attr('class', 'fa fa-check-circle-o fa-lg acl-permission__success'),
    'Render icon is correctly when the user is approved '
  );
  assert.equal(
    this.$('.acl-permission__meta')
      .children('p')
      .text()
      .trim(),
    'You now have access to this data',
    'Render request form subtitle 2 correctly'
  );
  assert.equal(this.$('.dataset-author-record').length, 2, 'Render table body correctly');
});

test('it should invoke the reset action on cancel', function(assert) {
  this.setProperties({
    accessInfo: accessInfo(false),
    currentUser,
    accessResponse,
    requestReason: 'request access'
  });

  this.render(hbs`
  {{dataset-aclaccess
    accessInfo=accessInfo
    currentUser=currentUser
    requestReason=requestReason}}`);

  assert.equal(this.get('requestReason'), 'request access');

  triggerEvent('.nacho-button--secondary', 'click');

  assert.equal(this.get('requestReason'), '');
});
