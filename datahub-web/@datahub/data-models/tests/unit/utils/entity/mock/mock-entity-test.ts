import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
import { module, test } from 'qunit';
import { IEntityLinkAttrs } from '@datahub/data-models/types/entity/shared';

module('Unit | Utility | entity/mock/mock-entity', function(): void {
  test('entityTabLink', function(assert): void {
    const expectedTab = 'tabName';
    const expectedUrn = 'mock-urn';
    const tabLinkParams = new MockEntity('mock-urn').entityTabLink(expectedTab) as IEntityLinkAttrs;

    assert.ok(tabLinkParams, 'Expected tabLinkParams to be generated when supplied with required attributes');

    const [, urn, tabName] = tabLinkParams.link.model as Array<string>;

    assert.equal(expectedTab, tabName, 'Expected the supplied tab name to be in the model array');
    assert.equal(expectedUrn, urn, 'Expected the supplied urn to be in the model array');
  });
});
