import { moduleFor, test } from 'ember-qunit';

moduleFor('route:datasets/dataset/schema', 'Unit | Route | datasets/dataset/schema', {
  needs: ['service:metrics']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
