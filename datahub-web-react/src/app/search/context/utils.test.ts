import { getMatchesPrioritizingPrimary } from './utils';

const MOCK_MATCHED_FIELDS = [
    {
        name: 'fieldPaths',
        value: 'rain',
    },
    {
        name: 'fieldDescriptions',
        value: 'rainbow',
    },
    {
        name: 'fieldPaths',
        value: 'rainbow',
    },
    {
        name: 'fieldPaths',
        value: 'rainbows',
    },
];

describe('utils', () => {
    describe('getMatchPrioritizingPrimary', () => {
        it('prioritizes exact match', () => {
            global.window.location.search = 'query=rainbow';
            const groupedMatches = getMatchesPrioritizingPrimary(MOCK_MATCHED_FIELDS, 'fieldPaths');
            expect(groupedMatches).toEqual([
                {
                    fieldName: 'fieldPaths',
                    matchedFields: [
                        { name: 'fieldPaths', value: 'rainbow' },
                        { name: 'fieldPaths', value: 'rainbows' },
                        { name: 'fieldPaths', value: 'rain' },
                    ],
                },
                {
                    fieldName: 'fieldDescriptions',
                    matchedFields: [{ name: 'fieldDescriptions', value: 'rainbow' }],
                },
            ]);
        });
        it('will accept first contains match', () => {
            global.window.location.search = 'query=bow';
            const groupedMatches = getMatchesPrioritizingPrimary(MOCK_MATCHED_FIELDS, 'fieldPaths');
            expect(groupedMatches).toEqual([
                {
                    fieldName: 'fieldPaths',
                    matchedFields: [
                        { name: 'fieldPaths', value: 'rainbow' },
                        { name: 'fieldPaths', value: 'rainbows' },
                        { name: 'fieldPaths', value: 'rain' },
                    ],
                },
                {
                    fieldName: 'fieldDescriptions',
                    matchedFields: [{ name: 'fieldDescriptions', value: 'rainbow' }],
                },
            ]);
        });
    });
});
