import { ActionRequestStatus, ActionRequestType, FilterOperator } from '@src/types.generated';
import { mergeFilters, entityHasProposals } from '../utils';

const MOCK_ENTITY_DATA = {
    proposals: [
        {
            created: {
                time: 1710324000000,
            },
            status: ActionRequestStatus.Completed,
            type: ActionRequestType.TagAssociation,
            urn: 'urn:example:tag:1',
        },
        {
            created: {
                time: 1612396473001,
            },
            status: ActionRequestStatus.Completed,
            type: ActionRequestType.TermAssociation,
            urn: 'urn:example:term:1',
        },
    ],
};

const MOCK_BASE_FILTERS = [
    {
        field: 'createdBy',
        values: ['urn:corpUser:admin'],
        negated: false,
        condition: FilterOperator.Equal,
    },
];

describe('Proposal utils', () => {
    describe('entityHasFilters', () => {
        it('returns false for empty entity data', () => {
            expect(entityHasProposals(null)).toEqual(false);
        });
        it("returns false when the entity doesn't have proposals", () => {
            expect(entityHasProposals({ proposals: [] })).toEqual(false);
        });
        it("returns true when the entity doesn't have proposals", () => {
            expect(entityHasProposals(MOCK_ENTITY_DATA)).toEqual(true);
        });
    });

    describe('mergeFilters', () => {
        it('returns the base filters for empty newFilters', () => {
            expect(mergeFilters(MOCK_BASE_FILTERS, [])).toEqual(MOCK_BASE_FILTERS);
        });
        it('merges the filters if same field is found', () => {
            const newFiltersWithSameField = [
                {
                    field: 'createdBy',
                    values: ['urn:corpUser:test1', 'urn:corpUser:test2'],
                    negated: false,
                    condition: FilterOperator.Equal,
                },
            ];
            const mergedFilters = mergeFilters(MOCK_BASE_FILTERS, newFiltersWithSameField);
            expect(mergedFilters.length).toEqual(1);
            expect(mergedFilters[0]?.values?.length).toEqual(3);
        });
        it('appends the filters if fields are different', () => {
            const newFiltersWithDifferentFields = [
                {
                    field: 'status',
                    values: [ActionRequestStatus.Pending],
                    negated: false,
                    condition: FilterOperator.Equal,
                },
                {
                    field: 'type',
                    values: [ActionRequestType.TagAssociation],
                    negated: false,
                    condition: FilterOperator.Equal,
                },
            ];

            const mergedFilters2 = mergeFilters(MOCK_BASE_FILTERS, newFiltersWithDifferentFields);
            expect(mergedFilters2.length).toEqual(3);
            expect(mergedFilters2[0]?.values?.length).toEqual(1);
        });
    });
});
