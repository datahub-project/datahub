import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

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

// TODO: Test the data-aclaccess-users component is working.
test('it renders', function(assert) {
  const className = '.dataset-author-record';

  this.setProperties({
    user: user,
    addOwner: function() {}
  });

  this.render(hbs`{{dataset-aclaccess-users
                    user=user
                    addOwner=addOwner}}`);

  assert.ok(this.$(), 'Render without errors');

  assert.equal(document.querySelector(className).tagName, 'TR', 'Component wrapper is <tr> tag');

  assert.equal(this.get('user'), user, 'user property should equal to user');
});
