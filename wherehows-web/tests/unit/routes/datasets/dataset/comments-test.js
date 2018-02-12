import { moduleFor, test } from 'ember-qunit';

moduleFor('route:datasets/dataset/comments', 'Unit | Route | datasets/dataset/comments', {
  needs: ['service:metrics']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
