import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { DefaultTableClasses } from '@nacho-ui/core/constants/nacho-table/default-table-properties';
import { INachoTableConfigs, NachoTableComputedLink } from '@nacho-ui/core/types/nacho-table';

module('Integration | Component | nacho-table', function(hooks) {
  setupRenderingTest(hooks);

  interface IBasicData {
    name: string;
    type: string;
    nature: string;
  }

  const titleClass = `.${DefaultTableClasses.title}`;
  const bodyClass = `.${DefaultTableClasses.body}`;
  const rowClass = `.${DefaultTableClasses.row}`;
  const cellClass = `.${DefaultTableClasses.cell}`;
  const basicData: Array<IBasicData> = [
    { name: 'Bulbasaur', type: 'Grass', nature: 'Hardy' },
    { name: 'Charmander', type: 'Fire', nature: 'Timid' },
    { name: 'Squirtle', type: 'Water', nature: 'Impish' }
  ];
  const testConfigs: INachoTableConfigs<IBasicData> = {
    headers: [{ title: 'Names', className: 'pokemon-name' }, { title: 'Types' }, { title: 'Natures' }],
    labels: ['name', 'type', 'nature'],
    customColumns: {
      name: {
        className: 'custom-name-class'
      }
    }
  };

  test('it renders the most basic custom use case', async function(assert) {
    await render(hbs`{{nacho-table/nacho-table
                       tableClass="my-class-1 my-class-2"
                     }}`);
    assert.ok(true, 'Initial render is without errors');
    assert.equal(findAll('table.nacho-table.my-class-1.my-class-2').length, 1, 'Renders the expectecd class');

    assert.equal(findAll('.my-class-1__global').length, 1, 'Renders the expected global class');

    this.set('basicData', basicData);

    await render(hbs`
      {{#nacho-table/nacho-table
        data=basicData
        as |table|
      }}
        {{#table.global}}
          <h1>Starter Pokemon</h1>
        {{/table.global}}
        {{#table.head as |head|}}
          {{#head.title}}
            Name
          {{/head.title}}
          {{#head.title}}
            Type
          {{/head.title}}
          {{#head.title}}
            Nature
          {{/head.title}}
        {{/table.head}}

        {{#table.body as |body|}}
          {{#each table.fields as |field|}}
            {{#body.row as |row|}}
              {{#row.cell}}
                {{field.name}}
              {{/row.cell}}
              {{#row.cell}}
                {{field.type}}
              {{/row.cell}}
              {{#row.cell}}
                {{field.nature}}
              {{/row.cell}}
            {{/body.row}}
          {{/each}}
        {{/table.body}}
      {{/nacho-table/nacho-table}}
    `);

    assert.ok(true, 'Still renders without errors');
    assert.equal(findAll('table').length, 1, 'Renders a single table');
    assert.equal(findAll(titleClass).length, 3, 'Renders three headers');
    assert.equal(findAll(rowClass).length, 3, 'Renders 3 rows');

    const firstCell = find(`${bodyClass} ${rowClass}:first-child ${cellClass}:first-child`) as Element;

    assert.equal((firstCell.textContent || '').trim(), 'Bulbasaur', 'Content renders first row, first cell correctly');

    const secondRowThirdCell = find(`${bodyClass} ${rowClass}:nth-child(2) ${cellClass}:nth-child(3)`) as Element;

    assert.equal(
      (secondRowThirdCell.textContent || '').trim(),
      'Timid',
      'Content renders second row, third cell correctly'
    );
  });

  test('it renders the most basic config use case', async function(assert) {
    const testClass = '.custom-name-class';

    await render(hbs`{{nacho-table/nacho-table}}`);
    assert.ok(true, 'Initial render is without errors');

    this.setProperties({ basicData, testConfigs });

    await render(hbs`
    {{nacho-table/nacho-table
      data=basicData
      tableConfigs=testConfigs
      tableClass="my-special-table my-other-special-table"
    }}`);
    assert.equal(findAll(testClass).length, 3, 'Renders three cells under "name" column with custom class');
    assert.equal(findAll(titleClass).length, 3, 'Renders three headers');
    assert.equal(findAll(rowClass).length, 3, 'Renders 3 rows');

    const secondRowFirstCell = find(`${bodyClass} ${rowClass}:nth-child(2) ${cellClass}:first-child`) as Element;

    assert.equal(
      (secondRowFirstCell.textContent || '').trim(),
      'Charmander',
      'Content renders second row, first cell correctly'
    );

    const thirdRowSecondCell = find(`${bodyClass} ${rowClass}:nth-child(3) ${cellClass}:nth-child(2)`) as Element;

    assert.equal(
      (thirdRowSecondCell.textContent || '').trim(),
      'Water',
      'Content renders third row, second cell correctly'
    );
  });

  test('it renders computed links', async function(assert) {
    const testConfigs: INachoTableConfigs<IBasicData> = {
      headers: [{ title: 'Names' }, { title: 'Types' }, { title: 'Natures' }, { title: 'Trainer' }],
      labels: ['name', 'type', 'nature', 'trainer'],
      customColumns: {
        name: {
          displayLink: {
            className: 'test-table-display-link',
            isNewTab: true,
            compute(rowData): NachoTableComputedLink {
              return {
                ref: `https://bulbapedia.bulbagarden.net/wiki/${rowData.name}_(Pokemon)`,
                display: rowData.name
              };
            }
          }
        }
      }
    };
    this.setProperties({ basicData, testConfigs });

    await render(hbs`{{nacho-table/nacho-table
      data=basicData
      tableConfigs=testConfigs
    }}
    `);
    assert.equal(findAll('a').length, 3, 'Renders 3 links');
    assert.equal(findAll(titleClass).length, 4, 'Renders four headers');
    assert.equal(findAll(rowClass).length, 3, 'Renders 3 rows');

    const firstLink = find('a:first-child') as HTMLLinkElement;

    assert.equal((firstLink.textContent || '').trim(), 'Bulbasaur', 'Renders first link content correctly');

    assert.equal(
      firstLink.href,
      `https://bulbapedia.bulbagarden.net/wiki/Bulbasaur_(Pokemon)`,
      'Renders a element with correct computed href'
    );
  });
});
