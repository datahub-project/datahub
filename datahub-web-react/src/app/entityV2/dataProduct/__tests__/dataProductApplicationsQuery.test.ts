import { FragmentDefinitionNode } from 'graphql';

import { GetDataProductDocument } from '@graphql/dataProduct.generated';

// Regression guard for the Applications sidebar section on Data Products.
// SidebarApplicationSection is wired into DataProductEntity, but it only renders when the
// getDataProduct query actually fetches `applications`. That fetch was missing, so the
// section silently rendered empty. If the `applications { ...entityApplication }` selection
// is dropped from the query again, entityApplication stops being merged into the document
// and this test fails.
describe('getDataProduct query', () => {
    it('fetches applications so the Data Product Applications sidebar section is populated', () => {
        const fragmentNames = GetDataProductDocument.definitions
            .filter((def): def is FragmentDefinitionNode => def.kind === 'FragmentDefinition')
            .map((def) => def.name.value);

        expect(fragmentNames).toContain('entityApplication');
    });
});
