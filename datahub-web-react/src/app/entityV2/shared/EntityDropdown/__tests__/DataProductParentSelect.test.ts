import { filterResultsForMove } from '@app/entityV2/shared/EntityDropdown/DataProductParentSelect';

import { DataProduct, EntityType } from '@types';

const selfUrn = 'urn:li:dataProduct:self';

function dataProduct(urn: string, ancestorUrns: string[] = []): DataProduct {
    return {
        urn,
        type: EntityType.DataProduct,
        parentDataProducts: ancestorUrns.map((ancestorUrn) => ({
            urn: ancestorUrn,
            type: EntityType.DataProduct,
            parentDataProducts: [],
        })),
    } as DataProduct;
}

describe('filterResultsForMove', () => {
    it('should return true if the given data product is unrelated to the entity being moved', () => {
        expect(filterResultsForMove(dataProduct('urn:li:dataProduct:other'), selfUrn)).toBe(true);
    });

    it('should return false if the given data product is the entity being moved', () => {
        expect(filterResultsForMove(dataProduct(selfUrn), selfUrn)).toBe(false);
    });

    it('should return false if the given data product lists the entity as an ancestor', () => {
        expect(filterResultsForMove(dataProduct('urn:li:dataProduct:child', [selfUrn]), selfUrn)).toBe(false);
    });
});
