import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { ComplianceFieldIdValue } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';

module('Integration | Helper | compliance/is-none', function(hooks): void {
  setupRenderingTest(hooks);

  test('it gives the correct boolean', async function(assert): Promise<void> {
    this.set('inputValue', ComplianceFieldIdValue.None);

    await render(hbs`{{#if (compliance/is-none inputValue)}}
                       Pikachu
                     {{/if}}`);

    assert.equal(this.element.textContent?.trim(), 'Pikachu', 'Truthy case passes');

    this.set('inputValue', ComplianceFieldIdValue.MemberId);

    await render(hbs`{{#if (compliance/is-none inputValue)}}
                       Pikachu
                     {{/if}}`);

    assert.equal(this.element.textContent?.trim(), '', 'Falsy case passes');
  });
});
