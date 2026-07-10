import { print } from 'graphql';

import { GetDataProductDocument } from '@graphql/dataProduct.generated';

describe('dataProduct query includes applications field', () => {
    it('should request the applications field with entityApplication fragment', () => {
        const queryText = print(GetDataProductDocument);
        expect(queryText).toContain('applications');
        expect(queryText).toContain('entityApplication');
    });
});
