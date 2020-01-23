import { module, test } from 'qunit';
import { visit, currentURL, find, fillIn, click, findAll, waitFor } from '@ember/test-helpers';
import { setupApplicationTest, skip } from 'ember-qunit';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';
import {
  searchBarSelector,
  searchEntitySelector,
  searchButton
} from 'wherehows-web/tests/helpers/search/global-search-constants';
import { querySelector } from '@datahub/utils/test-helpers/dom-helpers';
import { TestContext } from 'ember-test-helpers';
import { fabricsArray } from '@datahub/data-models/entity/dataset/utils/urn';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

const getCheckboxSelector = (name: string): string => `.search-facet__option--${name} input`;
const getCheckbox = (test: TestContext, name: string): HTMLInputElement | undefined => {
  try {
    return querySelector<HTMLInputElement>(test, getCheckboxSelector(name));
  } catch (e) {
    return;
  }
};

const getCheckboxes = (test: TestContext): Record<string, HTMLInputElement> => {
  return fabricsArray.reduce((checkboxes: Record<string, HTMLInputElement>, name: string): Record<
    string,
    HTMLInputElement
  > => {
    const checkbox = getCheckbox(test, name);
    return checkbox
      ? {
          ...checkboxes,
          [name]: checkbox
        }
      : checkboxes;
  }, {});
};
const browseCardElement = '.browse-card-container';

module('Acceptance | search', function(hooks): void {
  setupApplicationTest(hooks);

  test('Search does not through an error when typing', async function(assert): Promise<void> {
    await appLogin();
    await visit('/');

    assert.equal(currentURL(), '/browse', 'Visiting index route redirects to /browse sub-route');

    const testEntityCardElement = find(browseCardElement) as HTMLElement;
    await click(testEntityCardElement);

    fillIn(searchBarSelector, 'Hello darkness my old friend');
    assert.ok(true, 'Did not encounter an error when filling in search bar');
  });

  test('visiting /search and restoring facet selections', async function(assert): Promise<void> {
    await appLogin();
    await visit('/search?facets=(origin%3AList(prod%2Ccorp))&keyword=car');

    assert.equal(currentURL(), '/search?facets=(origin%3AList(prod%2Ccorp))&keyword=car');

    const { prod, corp } = getCheckboxes(this);
    const searchBar = find(searchBarSelector) as HTMLInputElement;

    assert.ok(prod.checked);
    assert.ok(corp.checked);
    assert.equal(searchBar.value, 'car');

    await click(getCheckboxSelector('corp'));

    assert.equal(currentURL(), '/search?entity=datasets&facets=(origin%3AList(prod))&keyword=car&page=1');
  });

  test('visiting /search and getting no results', async function(assert): Promise<void> {
    await appLogin();
    await visit('/search?keyword=somethingwickedthiswaycomes');

    assert.equal(currentURL(), '/search?keyword=somethingwickedthiswaycomes');

    const searchBar = find(searchBarSelector) as HTMLInputElement;
    assert.equal(searchBar.value, 'somethingwickedthiswaycomes');
    assert.equal(findAll('.empty-state').length, 1, 'Renders an empty state component when no results');
  });

  skip('default facets', async function(assert): Promise<void> {
    await appLogin();
    await visit('/browse');

    await waitFor(searchEntitySelector);
    fillIn(searchEntitySelector, DatasetEntity.displayName);

    await waitFor(searchBarSelector);
    fillIn(searchBarSelector, 'me');

    await click(searchButton);

    assert.equal(currentURL(), '/search?entity=datasets&facets=(status%3AList(published))&keyword=me');

    const publishedFacet = getCheckbox(this, 'published');

    assert.ok(publishedFacet && publishedFacet.checked, 'Published shows selected even if there is no data');
  });
});
