import { moduleFor, test } from 'ember-qunit';

moduleFor('route:datasets/dataset/sample', 'Unit | Route | datasets/dataset/sample', {
  needs: ['service:metrics']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
