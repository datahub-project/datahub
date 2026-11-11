import { getMatchedFieldNames } from '@app/search/matches/utils';

import { EntityType } from '@types';

describe('getMatchedFieldNames', () => {
    it('groups an id match with name so the title highlighter sees it', () => {
        const matchedFieldNames = getMatchedFieldNames(EntityType.SchemaField, 'name');
        expect(matchedFieldNames).toContain('id');
        expect(matchedFieldNames).toContain('name');
    });
});
