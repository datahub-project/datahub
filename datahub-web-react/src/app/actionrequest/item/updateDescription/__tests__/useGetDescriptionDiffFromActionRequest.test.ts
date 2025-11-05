import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useGetDescriptionDiffFromActionRequest } from '@app/actionrequest/item/updateDescription/utils';
import { tryExtractSubResourceDescription } from '@src/app/entityV2/shared/utils';

import { ActionRequest, ActionRequestType, EntityType, SubResourceType } from '@types';

// Mock the tryExtractSubResourceDescription function
vi.mock('@src/app/entityV2/shared/utils', () => ({
    tryExtractSubResourceDescription: vi.fn(),
}));

const mockTryExtractSubResourceDescription = tryExtractSubResourceDescription as ReturnType<typeof vi.fn>;

describe('useGetDescriptionDiffFromActionRequest', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return empty strings when actionRequest has no params or entity', () => {
        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: undefined,
        } as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(result.current).toEqual({
            oldDescription: '',
            newDescription: '',
        });
    });

    it('should return new description from updateDescriptionProposal', () => {
        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
                properties: { description: 'old description' },
            },
            params: {
                updateDescriptionProposal: {
                    description: 'new description',
                },
            },
        } as unknown as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(result.current).toEqual({
            oldDescription: 'old description',
            newDescription: 'new description',
        });
    });

    it('should return empty new description when updateDescriptionProposal is missing', () => {
        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
                properties: { description: 'old description' },
            },
        } as unknown as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(result.current).toEqual({
            oldDescription: 'old description',
            newDescription: '',
        });
    });

    it('should get old description from editableProperties when available', () => {
        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
                properties: { description: 'old description in properties' },
                editableProperties: { description: 'old description in editableProperties' },
            },
            params: {
                updateDescriptionProposal: {
                    description: 'new description',
                },
            },
        } as unknown as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(result.current).toEqual({
            oldDescription: 'old description in editableProperties',
            newDescription: 'new description',
        });
    });

    it('should fallback to properties for old description when editableProperties is missing', () => {
        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
                properties: { description: 'old description in properties' },
            },
            params: {
                updateDescriptionProposal: {
                    description: 'new description',
                },
            },
        } as unknown as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(result.current).toEqual({
            oldDescription: 'old description in properties',
            newDescription: 'new description',
        });
    });

    it('should return empty old description when both editableProperties and properties are missing', () => {
        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
            },
            params: {
                updateDescriptionProposal: {
                    description: 'new description',
                },
            },
        } as unknown as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(result.current).toEqual({
            oldDescription: '',
            newDescription: 'new description',
        });
    });

    it('should get old description from subResource when subResource exists', () => {
        mockTryExtractSubResourceDescription.mockReturnValue('sub resource description');

        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
                properties: { description: 'entity description' },
            },
            subResource: 'field1',
            subResourceType: SubResourceType.DatasetField,
            params: {
                updateDescriptionProposal: {
                    description: 'new description',
                },
            },
        } as unknown as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(mockTryExtractSubResourceDescription).toHaveBeenCalledWith(
            mockActionRequest.entity,
            mockActionRequest.subResource,
        );
        expect(result.current).toEqual({
            oldDescription: 'sub resource description',
            newDescription: 'new description',
        });
    });

    it('should return empty old description when subResource exists but tryExtractSubResourceDescription returns undefined', () => {
        mockTryExtractSubResourceDescription.mockReturnValue(undefined);

        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
                properties: { description: 'entity description' },
            },
            subResource: 'field1',
            subResourceType: SubResourceType.DatasetField,
            params: {
                updateDescriptionProposal: {
                    description: 'new description',
                },
            },
        } as unknown as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(mockTryExtractSubResourceDescription).toHaveBeenCalledWith(
            mockActionRequest.entity,
            mockActionRequest.subResource,
        );
        expect(result.current).toEqual({
            oldDescription: '',
            newDescription: 'new description',
        });
    });

    it('should handle action request without params but with entity and subResource', () => {
        mockTryExtractSubResourceDescription.mockReturnValue('sub resource description');

        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
            },
            subResource: 'field1',
            subResourceType: SubResourceType.DatasetField,
        } as unknown as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(result.current).toEqual({
            oldDescription: 'sub resource description',
            newDescription: '',
        });
    });

    it('should handle action request with different entity types', () => {
        const mockActionRequest: ActionRequest = {
            urn: 'urn:li:actionRequest:test',
            type: ActionRequestType.UpdateDescription,
            status: 'PENDING',
            entity: {
                urn: 'urn:li:dashboard:test',
                type: EntityType.Dashboard,
                properties: { description: 'dashboard old description' },
            },
            params: {
                updateDescriptionProposal: {
                    description: 'dashboard new description',
                },
            },
        } as unknown as ActionRequest;

        const { result } = renderHook(() => useGetDescriptionDiffFromActionRequest(mockActionRequest));

        expect(result.current).toEqual({
            oldDescription: 'dashboard old description',
            newDescription: 'dashboard new description',
        });
    });
});
