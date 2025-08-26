import {
    createBatchProposalActionEvent,
    entityHasProposals,
    getProposalsCountByType,
    mergeFilters,
    replaceFilterValues,
} from '@app/taskCenterV2/proposalsV2/utils';
import { ActionRequest, ActionRequestStatus, ActionRequestType, FilterOperator } from '@src/types.generated';

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

    describe('replaceFilterValues', () => {
        it('replaces the values if same field is found', () => {
            const newFiltersWithSameField = [
                {
                    field: 'createdBy',
                    values: ['urn:corpUser:test1', 'urn:corpUser:test2'],
                    negated: false,
                    condition: FilterOperator.Equal,
                },
            ];
            const mergedFilters = replaceFilterValues(MOCK_BASE_FILTERS, newFiltersWithSameField);
            expect(mergedFilters.length).toEqual(1);
            expect(mergedFilters[0]?.values?.length).toEqual(2);
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

            const mergedFilters2 = replaceFilterValues(MOCK_BASE_FILTERS, newFiltersWithDifferentFields);
            expect(mergedFilters2.length).toEqual(3);
            expect(mergedFilters2[0]?.values?.length).toEqual(1);
        });
    });
});

describe('Proposals Utils', () => {
    describe('getProposalsCountByType', () => {
        it('should return empty object for empty proposals array', () => {
            const proposals: ActionRequest[] = [];
            const result = getProposalsCountByType(proposals);
            expect(result).toEqual({});
        });

        it('should count proposals by type correctly', () => {
            const proposals: ActionRequest[] = [
                { type: ActionRequestType.TagAssociation, status: ActionRequestStatus.Pending } as ActionRequest,
                { type: ActionRequestType.TagAssociation, status: ActionRequestStatus.Pending } as ActionRequest,
                { type: ActionRequestType.TermAssociation, status: ActionRequestStatus.Pending } as ActionRequest,
            ];

            const result = getProposalsCountByType(proposals);
            expect(result).toEqual({
                [ActionRequestType.TagAssociation]: 2,
                [ActionRequestType.TermAssociation]: 1,
            });
        });
    });

    describe('createBatchProposalActionEvent', () => {
        it('should create event with correct structure for empty proposals', () => {
            const selectedProposals: ActionRequest[] = [];
            const result = createBatchProposalActionEvent('ProposalsAccepted', selectedProposals);

            expect(result).toEqual({
                actionType: 'ProposalsAccepted',
                entityUrns: [],
                proposalsCount: 0,
                countByType: {},
            });
        });

        it('should create event with correct structure for proposals with entities', () => {
            const selectedProposals: ActionRequest[] = [
                {
                    type: ActionRequestType.TagAssociation,
                    status: ActionRequestStatus.Pending,
                    entity: { urn: 'urn:li:dataset:1' },
                } as ActionRequest,
                {
                    type: ActionRequestType.TagAssociation,
                    status: ActionRequestStatus.Pending,
                    entity: { urn: 'urn:li:dataset:2' },
                } as ActionRequest,
                {
                    type: ActionRequestType.TermAssociation,
                    status: ActionRequestStatus.Pending,
                    entity: { urn: 'urn:li:dataset:3' },
                } as ActionRequest,
            ];

            const result = createBatchProposalActionEvent('ProposalsRejected', selectedProposals);

            expect(result).toEqual({
                actionType: 'ProposalsRejected',
                entityUrns: ['urn:li:dataset:1', 'urn:li:dataset:2', 'urn:li:dataset:3'],
                proposalsCount: 3,
                countByType: {
                    [ActionRequestType.TagAssociation]: 2,
                    [ActionRequestType.TermAssociation]: 1,
                },
            });
        });

        it('should filter out proposals without entity URNs', () => {
            const selectedProposals: ActionRequest[] = [
                {
                    type: ActionRequestType.TagAssociation,
                    status: ActionRequestStatus.Pending,
                    entity: { urn: 'urn:li:dataset:1' },
                } as ActionRequest,
                {
                    type: ActionRequestType.TagAssociation,
                    status: ActionRequestStatus.Pending,
                    entity: null,
                } as ActionRequest,
            ];

            const result = createBatchProposalActionEvent('ProposalsAccepted', selectedProposals);

            expect(result).toEqual({
                actionType: 'ProposalsAccepted',
                entityUrns: ['urn:li:dataset:1'],
                proposalsCount: 2,
                countByType: {
                    [ActionRequestType.TagAssociation]: 2,
                },
            });
        });
    });
});
