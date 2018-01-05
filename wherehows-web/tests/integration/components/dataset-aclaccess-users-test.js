import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import object from '@ember/object';

moduleForComponent('dataset-aclaccess-users', 'Integration | Component | dataset aclaccess users', {
  integration: true
});

const user = {
  userName: 'Mitchell_Rath',
  name: 'Crawford MacGyver',
  idType: 'USER',
  source: 'WP',
  modifiedTime: '2017-06-01T16:40:46.470Z',
  ownerShip: 'DataOwner'
};

test('it renders', function(assert) {
  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });
  const className = '.dataset-author-record';

  assert.expect(4);

  this.set('user', user);
  this.render(hbs`{{dataset-aclaccess-users user=user}}`);

  assert.ok(this.$(), 'Render without errors');

  assert.equal(document.querySelector(className).tagName, 'TR', 'Component wrapper is <tr> tag');

  assert.equal(this.get('user'), user, 'user property of  should equal to ');

  assert.equal(document.querySelector('.nacho-button').tagName, 'BUTTON', 'The Component includes a button');
});
