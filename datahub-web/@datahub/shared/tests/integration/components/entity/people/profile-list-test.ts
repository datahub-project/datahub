import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { IDynamicLinkParams } from 'dynamic-link/components/dynamic-link';
import { baseClass } from '@datahub/shared/components/entity/people/profile-list';

module('Integration | Component | entity/people/profile-list', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders and behaves as expected', async function(assert) {
    // Creates a partial instance of a person entity with the properties required for profile list
    // TODO META-9851: Allow for populateMockPersonEntity function to be importable
    // The package on which it lives on currently is not published on NPM registry
    const testPersonEntity: Partial<PersonEntity> = {
      name: 'Ash Ketchum',
      title: 'Pokemon master in training',
      profilePictureUrl: 'https://i.imgur.com/vjLcuFJ.jpg',
      email: 'ashketchumfrompallet@gmail.com'
    };
    const linkParam: IDynamicLinkParams = { href: 'https://linkedin.com', title: 'linkedin' };
    const profile = { entity: testPersonEntity, linkParam };
    const profiles = new Array(5).fill(profile);

    this.set('profiles', profiles);

    await render(hbs`
      <Entity::People::ProfileList
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
