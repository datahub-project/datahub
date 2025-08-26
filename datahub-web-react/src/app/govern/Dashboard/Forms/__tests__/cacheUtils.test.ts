import { updateFormAssignmentStatusInList, updateFormsList } from '@app/govern/Dashboard/Forms/cacheUtils';
import { GetSearchResultsForMultipleDocument } from '@src/graphql/search.generated';

import { AssignmentStatus } from '@types';

// Mock Apollo Client
const mockClient = {
    readQuery: vi.fn(),
    writeQuery: vi.fn(),
};

describe('cacheUtils', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('updateFormsList', () => {
        const mockInputs = { query: 'test' };
        const mockSearchAcrossEntities = {
            total: 1,
            searchResults: [],
        };

        const mockNewForm = {
            urn: 'urn:li:form:test',
            info: {
                name: 'Test Form',
                description: 'Test Description',
                type: 'TEST',
                status: 'ACTIVE',
                prompts: [],
                lastModified: 1234567890,
                created: 1234567890,
                actors: [],
            },
            ownership: {},
            dynamicFormAssignment: {},
            formAssignmentStatus: { status: AssignmentStatus.Complete, timestamp: 1234567890 },
        };

        it('should return early if no cached data exists', () => {
            mockClient.readQuery.mockReturnValue(null);

            updateFormsList(mockClient, mockInputs, mockNewForm, 'urn:li:form:test', mockSearchAcrossEntities, false);

            expect(mockClient.writeQuery).not.toHaveBeenCalled();
        });

        it('should add new form to cache when form does not exist', () => {
            const existingData = {
                searchAcrossEntities: {
                    searchResults: [],
                },
            };

            mockClient.readQuery.mockReturnValue(existingData);

            updateFormsList(mockClient, mockInputs, mockNewForm, 'urn:li:form:test', mockSearchAcrossEntities, false);

            expect(mockClient.writeQuery).toHaveBeenCalledWith({
                query: GetSearchResultsForMultipleDocument,
                variables: { input: mockInputs },
                data: {
                    searchAcrossEntities: {
                        ...mockSearchAcrossEntities,
                        total: 1,
                        searchResults: expect.arrayContaining([
                            expect.objectContaining({
                                entity: expect.objectContaining({
                                    urn: 'urn:li:form:test',
                                }),
                            }),
                        ]),
                    },
                },
            });
        });

        it('should update existing form in cache', () => {
            const existingForm = {
                entity: {
                    urn: 'urn:li:form:test',
                    type: 'FORM',
                    formInfo: {
                        name: 'Old Name',
                        description: 'Old Description',
                        type: 'TEST',
                        status: 'ACTIVE',
                        prompts: [],
                        lastModified: 1234567890,
                        created: 1234567890,
                        actors: [],
                    },
                    ownership: {},
                    dynamicFormAssignment: {},
                    formAssignmentStatus: { status: AssignmentStatus.Complete, timestamp: 1234567890 },
                },
                matchedFields: [],
                insights: [],
                extraProperties: [],
                __typename: 'SearchResult',
            };

            const existingData = {
                searchAcrossEntities: {
                    searchResults: [existingForm],
                },
            };

            mockClient.readQuery.mockReturnValue(existingData);

            updateFormsList(mockClient, mockInputs, mockNewForm, 'urn:li:form:test', mockSearchAcrossEntities, false);

            expect(mockClient.writeQuery).toHaveBeenCalledWith({
                query: GetSearchResultsForMultipleDocument,
                variables: { input: mockInputs },
                data: {
                    searchAcrossEntities: {
                        ...mockSearchAcrossEntities,
                        total: 1,
                        searchResults: expect.arrayContaining([
                            expect.objectContaining({
                                entity: expect.objectContaining({
                                    urn: 'urn:li:form:test',
                                    formInfo: expect.objectContaining({
                                        name: 'Test Form',
                                    }),
                                }),
                            }),
                        ]),
                    },
                },
            });
        });
    });

    describe('updateFormAssignmentStatusInList', () => {
        const mockFormUrn = 'urn:li:form:test';
        const mockFormAssignmentStatus = {
            status: AssignmentStatus.Complete,
            timestamp: 1234567890,
        };

        it('should return early if no cached data exists', () => {
            mockClient.readQuery.mockReturnValue(null);

            updateFormAssignmentStatusInList(mockClient, mockFormUrn, mockFormAssignmentStatus);

            expect(mockClient.writeQuery).not.toHaveBeenCalled();
        });

        it('should update form assignment status in cache', () => {
            const existingForm = {
                entity: {
                    urn: 'urn:li:form:test',
                    type: 'FORM',
                    formInfo: {
                        name: 'Test Form',
                        description: 'Test Description',
                        type: 'TEST',
                        status: 'ACTIVE',
                        prompts: [],
                        lastModified: 1234567890,
                        created: 1234567890,
                        actors: [],
                    },
                    ownership: {},
                    dynamicFormAssignment: {},
                    formAssignmentStatus: { status: AssignmentStatus.InProgress, timestamp: 1234567890 },
                },
                matchedFields: [],
                insights: [],
                extraProperties: [],
                __typename: 'SearchResult',
            };

            const existingData = {
                searchAcrossEntities: {
                    searchResults: [existingForm],
                },
            };

            mockClient.readQuery.mockReturnValue(existingData);

            updateFormAssignmentStatusInList(mockClient, mockFormUrn, mockFormAssignmentStatus);

            expect(mockClient.writeQuery).toHaveBeenCalledWith({
                query: GetSearchResultsForMultipleDocument,
                variables: { input: expect.any(Object) },
                data: {
                    searchAcrossEntities: {
                        total: 1,
                        searchResults: expect.arrayContaining([
                            expect.objectContaining({
                                entity: expect.objectContaining({
                                    urn: 'urn:li:form:test',
                                    formAssignmentStatus: mockFormAssignmentStatus,
                                }),
                            }),
                        ]),
                    },
                },
            });
        });
    });
});
