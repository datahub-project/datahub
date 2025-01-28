import { EntityType } from '../../../types.generated';
import {
    getDescriptionSlice,
    getGroupedFieldName,
    getMatchedFieldLabel,
    getMatchedFieldNames,
    getMatchedFieldsByNames,
    getMatchedFieldsByUrn,
    getMatchesPrioritized,
    isDescriptionField,
    isHighlightableEntityField,
    shouldShowInMatchedFieldList,
} from './utils';

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

    describe('shouldShowInMatchedFieldList', () => {
        it('should return true if field should show in matched field list', () => {
            const field = { name: 'fieldPaths', value: 'rainbow' };
            const show = shouldShowInMatchedFieldList(EntityType.Dashboard, field);
            expect(show).toBe(true);
        });

        it('should return false if field should not show in matched field list', () => {
            const field = { name: 'description', value: 'edited description value' };
            const show = shouldShowInMatchedFieldList(EntityType.Dashboard, field);
            expect(show).toBe(false);
        });
    });

    describe('getMatchedFieldLabel', () => {
        it('should return label for matched field', () => {
            const label = getMatchedFieldLabel(EntityType.Chart, 'fieldPaths');
            expect(label).toBe('column');
        });

        it('should return empty string if field name is not found', () => {
            const label = getMatchedFieldLabel(EntityType.Chart, 'unknownField');
            expect(label).toBe('');
        });
    });

    describe('getGroupedFieldName', () => {
        it('should return grouped field name for EntityType and fieldName', () => {
            const groupedFieldName = getGroupedFieldName(EntityType.Dashboard, 'name');
            console.log(groupedFieldName);
            expect(groupedFieldName).toBe('name');
        });

        it('should return undefined if field name is not found', () => {
            const groupedFieldName = getGroupedFieldName(EntityType.Dashboard, 'fieldPaths');
            expect(groupedFieldName).toBeUndefined();
        });
    });

    describe('getMatchedFieldNames', () => {
        it('should return matched field names for grouped field name', () => {
            const matchedFieldNames = getMatchedFieldNames(EntityType.Chart, 'fieldPaths');
            expect(matchedFieldNames).toContain('fieldPaths');
        });
    });

    describe('getMatchedFieldsByNames', () => {
        it('should return matched fields by names', () => {
            const fields = [
                { name: 'fieldPaths', value: 'rainbow' },
                { name: 'fieldDescriptions', value: 'rainbow' },
            ];
            const matchedFields = getMatchedFieldsByNames(fields, ['fieldPaths']);
            expect(matchedFields.length).toBe(1);
        });
    });

    describe('getMatchedFieldsByUrn', () => {
        it('should return matched fields by URN', () => {
            const fields = [
                { name: 'fieldPaths', value: 'urn:example:1' },
                { name: 'fieldPaths2', value: 'urn:example:2' },
            ];
            const matchedFields = getMatchedFieldsByUrn(fields, 'urn:example:1');
            expect(matchedFields.length).toBe(1);
        });
    });

    describe('isHighlightableEntityField', () => {
        it('should return true if field is highlightable', () => {
            const field = {
                name: 'fieldName',
                value: 'fieldValue',
                entity: { type: EntityType.GlossaryTerm, urn: 'GlossaryTermUrn' },
            };
            const isHighlightable = isHighlightableEntityField(field);
            expect(isHighlightable).toBe(true);
        });

        it('should return false if field is not highlightable', () => {
            const field = {
                name: 'fieldName',
                value: 'fieldValue',
                entity: { type: EntityType.Chart, urn: 'folderUrn' },
            };
            const isHighlightable = isHighlightableEntityField(field);
            expect(isHighlightable).toBe(false);
        });
    });

    describe('isDescriptionField', () => {
        it('should return true if field name contains "description"', () => {
            const field = { name: 'fieldDescriptions', value: 'description value' };
            const isDescField = isDescriptionField(field);
            expect(isDescField).toBe(true);
        });

        it('should return false if field name does not contain "description"', () => {
            const field = { name: 'fieldPaths', value: 'path value' };
            const isDescField = isDescriptionField(field);
            expect(isDescField).toBe(false);
        });
    });

    describe('getDescriptionSlice', () => {
        it('should return slice of text surrounding the target', () => {
            const text = 'This is a sample description text value';
            const target = 'description';
            const slice = getDescriptionSlice(text, target);
            expect(slice).toBe('... a sample description text valu...');
        });
    });
});
