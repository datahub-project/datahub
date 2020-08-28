import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IInstitutionalMemory } from '@datahub/metadata-types/types/aspects/institutional-memory';
import { baseTableClass } from '@datahub/shared/components/institutional-memory/wiki/url-list';
import { InstitutionalMemory } from '@datahub/data-models/models/aspects/institutional-memory';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

module('Integration | Component | institutional-memory/wiki/url-list', function(hooks): void {
  setupRenderingTest(hooks);

  const baseClass = `.${baseTableClass}`;

  test('it renders data as intended', async function(assert): Promise<void> {
    assert.expect(5);
    stubService('notifications', {
      notify: null
    });

    await render(hbs`{{institutional-memory/wiki/url-list}}`);
    assert.ok(this.element, 'Initial render is without errors');

    const listData: Array<IInstitutionalMemory> = [
      {
        url: 'https://www.serebii.net/pokedex-sm/025.shtml',
        description: 'Pikachu page',
        createStamp: { actor: 'aketchum', time: 1556561920 }
      },
      {
        url: 'https://www.serebii.net/pokedex-sm/133.shtml',
        description: 'Eevee page',
        createStamp: { actor: 'goak', time: 1556571920 }
      }
    ];

    this.set(
      'listData',
      listData.map(item => new InstitutionalMemory(item))
    );
    await render(hbs`{{institutional-memory/wiki/url-list
                       listData=listData
                     }}`);

    assert.equal(findAll(`${baseClass} .nacho-table__row`).length, 2, 'Renders 2 rows as expected');
    assert.equal(
      find(`${baseClass} .nacho-table__row:first-child td:first-child`)?.textContent?.trim(),
      'aketchum',
      'Renders author of a link as expected'
    );
    assert.equal(
      findAll(`${baseClass} .nacho-table__row:first-child td:first-child a`).length,
      1,
      'Renders a link for the user if a username/urn has been given'
    );

    const removeAction = (linkObject: InstitutionalMemory): void => {
      assert.equal(linkObject.url, 'https://www.serebii.net/pokedex-sm/133.shtml', 'Remove action works as expected');
    };

    this.set('removeAction', removeAction);

    await render(hbs`{{institutional-memory/wiki/url-list
                       listData=listData
                       removeInstitutionalMemoryLink=removeAction
                     }}`);

    await click(`${baseClass} .nacho-table__row:nth-child(2) ${baseClass}__actions-button`);
  });
});
