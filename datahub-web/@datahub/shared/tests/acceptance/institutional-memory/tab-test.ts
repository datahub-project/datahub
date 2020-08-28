import { module, test } from 'qunit';
import { visit, currentURL, find, findAll, click, waitFor, fillIn } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { IMirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { baseTableClass } from '@datahub/shared/components/institutional-memory/wiki/url-list';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

module('Acceptance | tab', function(hooks): void {
  setupApplicationTest(hooks);

  setupMirage(hooks);

  const baseClass = `.${baseTableClass}`;
  const rowClass = `${baseClass} .nacho-table__row`;
  const authorClass = `${baseClass}__author-info`;
  const modalClass = `${baseClass}-modal`;
  const modalUrlClass = `${modalClass}__url`;

  const firstRowAuthor = 'aketchum';
  const secondRowAuthor = 'goak';

  test('testing institutional-memory tab container', async function(this: IMirageTestContext, assert) {
    const server = this.server;

    server.createList('institutionalMemory', 2, 'static');

    stubService('notifications', {
      notify: null
    });

    await visit('/wiki');
    assert.equal(currentURL(), '/wiki');
    assert.equal(findAll(rowClass).length, 2, 'Renders 2 rows as expected');
    assert.equal(
      find(`${rowClass}:first-child ${authorClass}`)?.textContent?.trim(),
      firstRowAuthor,
      'Renders author of a link as expected'
    );

    await click(`${rowClass}:first-child ${baseClass}__actions-button`);
    assert.equal(findAll(rowClass).length, 1, 'Delete action removed a row');

    assert.equal(
      find(`${rowClass}:first-child ${authorClass}`)?.textContent?.trim(),
      secondRowAuthor,
      'Remaining author was the second one'
    );

    // Clicking on the "Add link" button
    await click(`${baseClass}__footer-button`);
    await waitFor(modalClass);

    assert.equal(findAll(modalClass).length, 1, 'Renders a modal upon clicking the add more link');
    assert.equal(findAll(modalUrlClass).length, 1, 'Renders a url input');
    assert.equal(findAll(`footer button`).length, 2, 'Renders two action buttons');

    await fillIn(modalUrlClass, 'someLink');
    await fillIn(`${modalClass}__description`, 'some description');
    // Click to save
    await click(`footer ${modalClass}__save`);

    assert.equal(findAll(rowClass).length, 2, 'We have two links again');
    assert.equal(
      find(`${rowClass}:nth-child(2) ${authorClass}`)?.textContent?.trim(),
      'pikachu',
      'Renders the new row as expected'
    );
    assert.equal(
      find(`${rowClass}:nth-child(2) ${baseClass}__description-info`)?.textContent?.trim(),
      'some description',
      'Renders descriptions as expected'
    );
  });
});
