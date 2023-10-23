import { EntityType } from '../../../types.generated';
import { getMatchesPrioritized } from './utils';

const mapping = new Map();
mapping.set('fieldPaths', 'column');
mapping.set('fieldDescriptions', 'column description');
mapping.set('fieldTags', 'column tag');

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

const MOCK_MATCHED_DESCRIPTION_FIELDS = [
    {
        name: 'editedDescription',
        value: 'edited description value',
    },
    {
        name: 'description',
        value: 'description value',
    },
    {
        name: 'fieldDescriptions',
        value: 'field descriptions value',
    },
    {
        name: 'editedFieldDescriptions',
        value: 'edited field descriptions value',
    },
];

describe('utils', () => {
    describe('getMatchPrioritizingPrimary', () => {
        it('prioritizes exact match', () => {
            global.window.location.search = 'query=rainbow';
            const groupedMatches = getMatchesPrioritized(EntityType.Dataset, MOCK_MATCHED_FIELDS, 'fieldPaths');
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
            const groupedMatches = getMatchesPrioritized(EntityType.Dataset, MOCK_MATCHED_FIELDS, 'fieldPaths');
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
        it('will group by field name', () => {
            global.window.location.search = '';
            const groupedMatches = getMatchesPrioritized(
                EntityType.Dataset,
                MOCK_MATCHED_DESCRIPTION_FIELDS,
                'fieldPaths',
            );
            expect(groupedMatches).toEqual([
                {
                    fieldName: 'description',
                    matchedFields: [
                        { name: 'editedDescription', value: 'edited description value' },
                        { name: 'description', value: 'description value' },
                    ],
                },
                {
                    fieldName: 'fieldDescriptions',
                    matchedFields: [
                        { name: 'fieldDescriptions', value: 'field descriptions value' },
                        { name: 'editedFieldDescriptions', value: 'edited field descriptions value' },
                    ],
                },
            ]);
        });
    });
});
