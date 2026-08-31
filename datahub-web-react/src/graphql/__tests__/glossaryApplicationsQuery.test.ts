import { print } from 'graphql';

import { GetGlossaryTermDocument } from '@graphql/glossaryTerm.generated';

describe('glossaryTerm query includes applications field', () => {
    it('should request the applications field with entityApplication fragment', () => {
        const queryText = print(GetGlossaryTermDocument);
        expect(queryText).toContain('applications');
        expect(queryText).toContain('entityApplication');
    });
});
