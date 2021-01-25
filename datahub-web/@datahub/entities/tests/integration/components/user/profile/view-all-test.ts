import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { getTextNoSpacesFromElement } from '@datahub/utils/test-helpers/dom-helpers';

module('Integration | Component | user/profile/view-all', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders as expected', async function(assert): Promise<void> {
    this.setProperties({
      title: 'sample title',
      tags: ['tag1', 'tag2', 'tag3', 'tag4', 'tag5']
    });

    await render(hbs`
      <User::Profile::ViewAll
        @title={{this.title}}
        @tags={{this.tags}}
      />
    `);

    const title = document.querySelector('.view-all__title') as Element;
    assert.equal(getTextNoSpacesFromElement(title), 'sampletitle');

    const content = document.querySelector('.view-all__content') as Element;
    assert.equal(getTextNoSpacesFromElement(content), 'tag1tag2tag3tag4tag5');
  });
});
