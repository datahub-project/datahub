import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { noop } from 'lodash';
import { baseTableClass } from '@datahub/shared/components/change-management/change-log-table';
import { ChangeLog } from '@datahub/shared/modules/change-log';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

const wikiLinks = {
  changeManagement: 'http://www.example.com'
};

module('Integration | Component | change-management/change-log-table', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function(): void {
    stubService('configurator', {
      getConfig(): object {
        return wikiLinks;
      }
    });
  });

  test('Owners only - Renders fine', async function(assert) {
    const changeLogs: Array<ChangeLog> = [
      {
        id: 1,
        createdBy: 'ranbalag',
        dateAdded: 15567894,
        subject: 'Test Subject here',
        content: 'Test Content here',
        sendEmail: false,
        owningEntity: {
          dataset: 'testUrn'
        }
      }
    ];
    this.setProperties({
      changeLogs,
      showChangeLogDetail: noop,
      sendNotification: noop,
      onAddLog: noop,
      isCurrentUserAnOwner: true
    });

    await render(hbs`
      <ChangeManagement::ChangeLogTable
        @changeLogs={{this.changeLogs}}
        @showChangeLogDetail={{action this.showChangeLogDetail}}
        @sendNotification={{action this.sendNotification}}
        @onAddLog={{action this.onAddLog}}
        @readOnly={{not this.isCurrentUserAnOwner}}
      />
    `);
    const tableClassSelector = `.${baseTableClass}`;
    const headerSelector = `.${baseTableClass}__global`;
    const footerIconSelector = `.${baseTableClass}__footer-icon`;
    const footerButtonSelector = `.${baseTableClass}__footer-button`;
    assert.dom(tableClassSelector).exists();
    // the extra +1 is ensuring the header is present
    assert.equal(findAll(`.${baseTableClass} tr`).length, changeLogs.length + 1);
    // ensure two actions exist for each change log
    assert.equal(findAll(`.${baseTableClass}__actions-button`).length, changeLogs.length * 2);
    // ensure the table provides a button to add a new log
    assert.dom(footerIconSelector).exists();
    assert.dom(footerButtonSelector).exists();
    assert.dom(`${headerSelector} .more-info`).exists('Expected the tooltip for wiki to be present');
    assert.dom(`${headerSelector} a`).hasAttribute('href', wikiLinks.changeManagement);
  });

  test('Non owners - Renders fine', async function(assert) {
    const changeLogs: Array<ChangeLog> = [
      {
        id: 1,
        createdBy: 'ranbalag',
        dateAdded: 15567894,
        subject: 'Test Subject here',
        content: 'Test Content here',
        sendEmail: false,
        owningEntity: {
          dataset: 'testUrn'
        }
      }
    ];
    this.setProperties({
      changeLogs,
      showChangeLogDetail: noop,
      sendNotification: noop,
      onAddLog: noop,
      isCurrentUserAnOwner: false
    });

    await render(hbs`
      <ChangeManagement::ChangeLogTable
        @changeLogs={{this.changeLogs}}
        @showChangeLogDetail={{action this.showChangeLogDetail}}
        @sendNotification={{action this.sendNotification}}
        @onAddLog={{action this.onAddLog}}
        @readOnly={{not this.isCurrentUserAnOwner}}
      />
    `);

    const tableClassSelector = `.${baseTableClass}`;
    const headerSelector = `.${baseTableClass}__global`;
    const footerIconSelector = `.${baseTableClass}__footer-icon`;
    const footerButtonSelector = `.${baseTableClass}__footer-button`;
    assert.dom(tableClassSelector).exists();
    // the extra +1 is ensuring the header is present
    assert.equal(findAll(`.${baseTableClass} tr`).length, changeLogs.length + 1);
    // ensure 1 actions exist for each change log
    assert.equal(findAll(`.${baseTableClass}__actions-button`).length, changeLogs.length * 1);
    // ensure the table to add a new log does not exist
    assert.dom(footerIconSelector).doesNotExist();
    assert.dom(footerButtonSelector).doesNotExist();
    assert.dom(`${headerSelector} .more-info`).exists('Expected the tooltip for wiki to be present');
    assert.dom(`${headerSelector} a`).hasAttribute('href', wikiLinks.changeManagement);
  });

  test('Owners only - it does not render send-email-button if email already sent', async function(assert) {
    const changeLogs: Array<ChangeLog> = [
      {
        id: 1,
        createdBy: 'ranbalag',
        dateAdded: 15567894,
        subject: 'Test Subject here',
        content: 'Test Content here',
        sendEmail: true,
        owningEntity: {
          dataset: 'testUrn'
        }
      }
    ];
    this.setProperties({
      changeLogs,
      showChangeLogDetail: noop,
      sendNotification: noop,
      onAddLog: noop,
      isCurrentUserAnOwner: true
    });

    await render(hbs`
      <ChangeManagement::ChangeLogTable
        @changeLogs={{this.changeLogs}}
        @showChangeLogDetail={{action this.showChangeLogDetail}}
        @sendNotification={{action this.sendNotification}}
        @onAddLog={{action this.onAddLog}}
        @readOnly={{not this.isCurrentUserAnOwner}}      />
    `);

    // 1 button for each log ensures that only view detail is present.
    assert.equal(findAll(`.${baseTableClass}__actions-button`).length, changeLogs.length);
  });
});
