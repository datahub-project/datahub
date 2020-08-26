import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IGridGroupEntity } from '@datahub/shared/types/grid-group';
import { IDynamicLinkParams } from 'dynamic-link/components/dynamic-link';
import { baseClass } from '@datahub/shared/components/entity/grid-group/profile-list';

module('Integration | Component | entity/grid-group/profile-list', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const testLink = 'https://linkedin.com';
    const testGridGroupEntity: IGridGroupEntity = {
      urn: 'urn:li:gridGroup:wherehow',
      name: 'wherehow',
      link: testLink
    };
    const linkParam: IDynamicLinkParams = { href: testLink, title: 'linkedin' };
    const profile = { entity: testGridGroupEntity, linkParam };
    const profiles = new Array(5).fill(profile);

    this.set('profiles', profiles);

    await render(hbs`
      <Entity::GridGroup::ProfileList
        @profiles={{this.profiles}}
      />
    `);

    const profileListClassSelector = `.${baseClass}`;

    assert.dom(profileListClassSelector).exists();
    assert.equal(
      document.querySelector(profileListClassSelector)?.childElementCount,
      profiles.length,
      'Expected amount of profiles rendered in the list to be equal to number of profiles passed in'
    );
  });
});
