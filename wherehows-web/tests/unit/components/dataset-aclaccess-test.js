import { moduleForComponent, test } from 'ember-qunit';
import Notifications from 'wherehows-web/services/notifications';
import { accessInfoTesting as accessInfo, accessState } from 'wherehows-web/constants/dataset-aclaccess';

moduleForComponent('dataset-aclaccess', 'Unit | Component | dataset aclaccess', {
  unit: true,
  needs: ['component:empty-state'],

  beforeEach() {
    this.register('service:notifications', Notifications);
  }
});

test('it renders', function(assert) {
  this.render();

  assert.ok(this.$(), 'Render without error');
});

test('should return the pageSate and users correctly', function(assert) {
  const aclaccess = this.subject();
  const approved = accessInfo(true);

  aclaccess.set('accessInfo', approved);

  assert.equal(aclaccess.get('pageState'), 'hasAcess', 'pageState without errors');

  assert.ok(aclaccess.get('users'), 'users existed');
});

test('should return state correctly', function(assert) {
  const aclaccess = this.subject();

  aclaccess.setProperties({
    pageState: 'noAccess',
    currentUser: 'Abc'
  });

  assert.equal(aclaccess.get('state').info, accessState('Abc').noAccess.info, 'pageState without errors');
});

test('should return isLoadForm correctly', function(assert) {
  const aclaccess = this.subject();

  aclaccess.set('pageState', 'noAccess');

  assert.ok(aclaccess.get('isLoadForm'), 'property of isLoadeForm return correctly');
});
