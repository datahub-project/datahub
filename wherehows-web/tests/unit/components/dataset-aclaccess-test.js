import { moduleForComponent, test } from 'ember-qunit';
import Notifications from 'wherehows-web/services/notifications';

moduleForComponent('dataset-aclaccess', 'Unit | Component | dataset aclaccess', {
  unit: true,
  needs: ['component:empty-state'],

  beforeEach() {
    this.register('service:notifications', Notifications);
  }
});

const accessInfo = permmision => {
  return {
    isAccess: permmision,
    body: [
      {
        id: 0,
        principal: 'urn:li:userPrincipal:Mitchell_rath',
        businessJustification: 'grandfathered in Mitchell_rath',
        accessTypes: ['READ', 'WRITE'],
        tableItem: {
          userName: 'Mitchell_Rath',
          name: 'Crawford MacGyver',
          idType: 'USER',
          source: 'WP',
          modifiedTime: '2017-06-01T16:40:46.470Z',
          ownerShip: 'DataOwner'
        }
      }
    ]
  };
};
const pageContent = userName => {
  return {
    success: {
      info: `${userName}, you have access to this data`,
      requestInfo: 'Congrats! Your request has been approved!',
      requestMessage: 'You now have a access to this data',
      classNameIcon: 'fa fa-check-circle-o fa-lg',
      classNameFont: 'acl-permission__success'
    },
    reject: {
      info: `${userName}, you currently do not have access to this dataset`,
      requestInfo: 'Sorry, you request has been denied by the system.',
      requestMessage: 'If you feel this is in error, contact acreqjests@linkedin.',
      classNameIcon: 'fa fa-ban fa-lg',
      classNameFont: 'acl-permission__reject'
    }
  };
};
const accessState = userName => {
  const content = pageContent(userName);
  return {
    hasAcess: {
      state: 'hasAcess',
      info: content.success.info,
      icon: content.success.classNameIcon,
      font: content.success.classNameFont
    },
    noAccess: {
      state: 'noAccess',
      info: content.reject.info,
      icon: content.reject.classNameIcon,
      font: content.reject.classNameFont
    },
    getAccess: {
      state: 'getAccess',
      info: content.success.requestInfo,
      message: content.success.requestMessage,
      icon: content.success.classNameIcon,
      font: content.success.classNameFont
    },
    denyAccess: {
      state: 'denyAccess',
      info: content.reject.requestInfo,
      message: content.reject.requestMessage,
      icon: content.reject.classNameIcon,
      font: content.reject.classNameFont
    }
  };
};
const approvedResponse = {
  principal: 'urn:li:userPrincipal:ABC',
  businessJustification: 'asdsd read',
  accessTypes: 'READ',
  tableItem: {
    userName: 'ABC',
    name: 'Solon Streich I',
    idType: 'USER',
    source: 'TY',
    modifiedTime: '2017-03-19T23:34:52.456Z',
    ownerShip: 'DataOwner'
  },
  id: 3
};

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
