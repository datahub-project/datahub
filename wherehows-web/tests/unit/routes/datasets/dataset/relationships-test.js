import { moduleFor, test } from 'ember-qunit';

moduleFor('route:datasets/dataset/relationships', 'Unit | Route | datasets/dataset/relationships', {
  needs: ['service:metrics']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
