import { moduleFor, test } from 'ember-qunit';

moduleFor('route:datasets/dataset/ownership', 'Unit | Route | datasets/dataset/ownership', {
  needs: ['service:metrics']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
