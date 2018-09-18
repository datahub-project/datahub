import { module, test } from 'qunit';
import { visit, currentURL, fillIn, click } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';
import { searchBarSelector } from 'wherehows-web/tests/helpers/search/global-search-constants';
import { querySelector } from 'wherehows-web/tests/helpers/dom-helpers';
import { TestContext } from 'ember-test-helpers';

const getCheckboxSelector = (name: string) => `.search-facet__option--${name} input`;
const getCheckbox = (test: TestContext, name: string) => {
  return querySelector<HTMLInputElement>(test, getCheckboxSelector(name));
};

const getCheckboxes = (test: TestContext): Record<string, HTMLInputElement> => {
  const envs = ['prod', 'corp', 'ei', 'dev'];
  return envs.reduce((checkboxes: Record<string, HTMLInputElement>, name: string) => {
    return {
      ...checkboxes,
      [name]: getCheckbox(test, name)
    };
  }, {});
};

module('Acceptance | search', function(hooks) {
  setupApplicationTest(hooks);

  test('Search does not through an error when typing', async function(assert) {
    await appLogin();
    await visit('/');

    assert.equal(currentURL(), '/browse/datasets', 'We made it to the home page in one piece');
    fillIn(searchBarSelector, 'Hello darkness my old friend');
    assert.ok(true, 'Did not encounter an error when filling in search bar');
  });

  test('visiting /search and restoring facet selections', async function(assert) {
    await appLogin();
    await visit('/search?facets=(fabric%3AList(prod%2Ccorp))&keyword=car');

    assert.equal(currentURL(), '/search?facets=(fabric%3AList(prod%2Ccorp))&keyword=car');

    const { prod, corp, dev, ei } = getCheckboxes(this);
    const searchBar = querySelector<HTMLInputElement>(this, searchBarSelector) || { value: '' };

    assert.ok(prod.checked);
    assert.ok(corp.checked);
    assert.notOk(dev.checked);
    assert.notOk(ei.checked);
    assert.equal(searchBar.value, 'car');

    await click(getCheckboxSelector('ei'));

    assert.equal(currentURL(), '/search?facets=(fabric%3AList(prod%2Ccorp%2Cei))&keyword=car&page=1');
  });
});
