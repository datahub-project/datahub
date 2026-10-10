import { print } from 'graphql';

import { GetDataFlowDocument } from '@graphql/dataFlow.generated';

describe('dataFlow query includes applications field', () => {
    it('should request the applications field with entityApplication fragment', () => {
        const queryText = print(GetDataFlowDocument);
        expect(queryText).toContain('applications');
        expect(queryText).toContain('entityApplication');
    });
});
