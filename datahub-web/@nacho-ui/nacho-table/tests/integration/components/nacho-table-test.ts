import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { DefaultTableClasses } from '@nacho-ui/table/constants/default-table-properties';

module('Integration | Component | nacho-table', function(hooks) {
  setupRenderingTest(hooks);

  const titleClass = `.${DefaultTableClasses.title}`;
  const bodyClass = `.${DefaultTableClasses.body}`;
  const rowClass = `.${DefaultTableClasses.row}`;
  const cellClass = `.${DefaultTableClasses.cell}`;

  test('it renders the most basic custom use case', async function(assert) {
    await render(hbs`{{nacho-table
                       tableClass="my-class-1 my-class-2"
                     }}`);
    assert.ok(true, 'Initial render is without errors');
    assert.equal(findAll('table.nacho-table.my-class-1.my-class-2').length, 1, 'Renders the expectecd class');

    assert.equal(findAll('.my-class-1__global').length, 1, 'Renders the expected global class');

    this.set('basicData', [
      { name: 'Bulbasaur', type: 'Grass', nature: 'Hardy' },
      { name: 'Charmander', type: 'Fire', nature: 'Timid' },
      { name: 'Squirtle', type: 'Water', nature: 'Impish' }
    ]);

    // This component provided by our dummy application has implemented the custom use case for the
    // nacho table. The template logic has been separated from this test as they can get very complex
    // and take away from our testing logic
    await render(hbs`{{custom-use-case/basic-table basicData=basicData}}`);

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

    await render(hbs`{{nacho-table}}`);
    assert.ok(true, 'Initial render is without errors');

    this.set('basicData', [
      { name: 'Bulbasaur', type: 'Grass', nature: 'Hardy' },
      { name: 'Charmander', type: 'Fire', nature: 'Timid' },
      { name: 'Squirtle', type: 'Water', nature: 'Impish' }
    ]);

    // This component provided by our dummy application has implemented the custom use case for the
    // nacho table. The template logic has been separated from this test as they can get very complex
    // and take away from our testing logic
    await render(hbs`{{config-cases/basic-case basicData=basicData}}`);
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
    this.set('basicData', [
      { name: 'Bulbasaur', type: 'Grass', nature: 'Hardy' },
      { name: 'Charmander', type: 'Fire', nature: 'Timid' },
      { name: 'Squirtle', type: 'Water', nature: 'Impish' }
    ]);

    // This component provided by our dummy application has implemented the custom use case for the
    // nacho table. The template logic has been separated from this test as they can get very complex
    // and take away from our testing logic
    await render(hbs`{{config-cases/display-links basicData=basicData}}`);
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
