import { withoutCoveredSubdomains } from '@app/entityV2/shared/DomainSelector/DomainSelector.utils';

const PARENT = 'urn:li:domain:parent';
const CHILD = 'urn:li:domain:child';
const GRANDCHILD = 'urn:li:domain:grandchild';
const OTHER = 'urn:li:domain:other';

const ANCESTORS = {
    [CHILD]: [PARENT],
    [GRANDCHILD]: [CHILD, PARENT],
};

describe('withoutCoveredSubdomains', () => {
    it('drops subdomains at any depth once an ancestor is selected', () => {
        expect(withoutCoveredSubdomains([CHILD, GRANDCHILD, OTHER, PARENT], ANCESTORS)).toEqual([OTHER, PARENT]);
    });

    it('keeps subdomains whose ancestors are not selected', () => {
        expect(withoutCoveredSubdomains([GRANDCHILD, OTHER], ANCESTORS)).toEqual([GRANDCHILD, OTHER]);
    });
});
