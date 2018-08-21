import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { splitText } from 'wherehows-web/helpers/split-text';

module('Integration | Component | datasets/urn-breadcrumbs/crumb', function(hooks) {
  setupRenderingTest(hooks);

  test('component rendering', async function(assert) {
    assert.expect(3);

    const crumb = {
      prefix: 'prefix',
      platform: 'platform',
      crumb: 'crumb'
    };

    /**
     * Starts the route instance, to allow href anchor attribute emit
     * @link https://github.com/emberjs/ember.js/issues/16904
     */
    this.owner.lookup('route:browse/entity')._router.setupRouter();

    this.set('crumb', crumb);

    await render(hbs`{{datasets/urn-breadcrumbs/crumb}}`);

    assert.equal(
      this.element.textContent!.trim(),
      '',
      ' expected component to render without errors when no crumb property is passed'
    );

    await render(hbs`{{datasets/urn-breadcrumbs/crumb crumb=crumb}}`);

    assert.equal(this.element.textContent!.trim(), crumb.crumb, 'expected component to render crumb property');
    assert.equal(
      this.element.querySelector('a')!.getAttribute('href'),
      '/browse/datasets?platform=platform&prefix=prefix',
      'expected anchor element href attribute to contain a link to crumb'
    );
  });

  test('rendering truncated text in breadcrumb', async function(assert) {
    assert.expect(1);

    const crumb = {
      crumb: 'thisisalongstringoftextthatexceedsthelimitforthiscrumb'
    };
    const maxCharLength = 12;

    this.setProperties({ crumb, maxCharLength });

    await render(hbs`{{datasets/urn-breadcrumbs/crumb crumb=crumb maxCrumbCharLength=maxCharLength}}`);

    assert.equal(
      this.element.textContent!.trim(),
      splitText([crumb.crumb, maxCharLength]),
      'expected text content of crumb to match truncated text'
    );
  });
});
