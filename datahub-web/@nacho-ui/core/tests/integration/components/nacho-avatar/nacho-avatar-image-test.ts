import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, waitUntil } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import Service from '@ember/service';
import NachoAvatarService from '@nacho-ui/core/services/nacho-avatars';

const imageClass = '.nacho-avatar';
const fallbackUrl = 'http://cdn.akc.org/content/hero/puppy-boundaries_header.jpg';
const imageUrl =
  'https://vignette.wikia.nocookie.net/starwars/images/2/20/LukeTLJ.jpg/revision/latest?cb=20170927034529';
const badUrl = 'thisdoesnotexist';
const altText = 'Luke Skywalker';

module('Integration | Component | nacho-avatar-image', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders when given a good url', async function(assert) {
    this.setProperties({ imageUrl, altText });
    await render(hbs`<NachoAvatar::NachoAvatarImage @img={{this.imageUrl}} @altText={{this.altText}}/>`);
    assert.ok(this.element, 'Initial render is without errors');

    const imageElement = find(imageClass) as Element;
    assert.ok(imageElement, 'Renders an image');
    assert.equal(imageElement.getAttribute('alt'), altText, 'Properly binds a alt attribute to the given image');
    assert.equal(imageElement.getAttribute('src'), imageUrl, 'Properly binds a src attribute to the given image');
  });

  test('image fallback', async function(assert) {
    const nachoStub: NachoAvatarService = (Service.extend({
      imgFallbackUrl: fallbackUrl
    }) as unknown) as NachoAvatarService;
    this.owner.register('service:nacho-avatars', nachoStub);

    this.set('imgUrl', badUrl);
    await render(hbs`<NachoAvatar::NachoAvatarImage @img={{this.imgUrl}} />`);
    await waitUntil(() => !!find(`${imageClass}[src="${fallbackUrl}"]`));

    assert.equal(
      (find(imageClass) as Element).getAttribute('src'),
      fallbackUrl,
      'src attribute is set to fallback url'
    );
  });
});
