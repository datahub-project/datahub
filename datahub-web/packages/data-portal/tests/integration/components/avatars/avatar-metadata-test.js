import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import avatars from 'wherehows-web/mirage/fixtures/avatars';

const avatar = avatars[0];

module('Integration | Component | avatars/avatar metadata', function(hooks) {
  setupRenderingTest(hooks);

  test('avatar-metadata rendering', async function(assert) {
    const expectedClassName = '.avatar-metadata';

    await render(hbs`{{avatars/avatar-metadata}}`);

    assert.ok(find(expectedClassName), 'it renders with the expected className');

    this.set('avatar', avatar);
    await render(hbs`
    {{#avatars/avatar-metadata avatar=avatar as |meta|}}
    {{meta.name}}{{meta.email}}{{meta.userName}}
    {{/avatars/avatar-metadata}}`);

    assert.equal(
      find(expectedClassName).textContent.trim(),
      `${avatar.name}${avatar.email}${avatar.userName}`,
      'contextual component yields expected avatar attributes'
    );
  });
});
