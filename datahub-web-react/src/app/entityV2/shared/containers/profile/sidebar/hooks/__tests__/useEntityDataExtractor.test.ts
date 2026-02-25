import { renderHook } from '@testing-library/react-hooks';

import { useEntityDataExtractor } from '@app/entityV2/shared/containers/profile/sidebar/hooks/useEntityDataExtractor';

const mockUseEntityData = vi.hoisted(() => vi.fn());

vi.mock('@src/app/entity/shared/EntityContext', async (importOriginal) => {
    const original = await importOriginal<object>();
    return {
        ...original,
        useEntityData: mockUseEntityData,
    };
});

describe('useEntityDataExtractor', () => {
    beforeEach(() => {
        mockUseEntityData.mockClear();
    });

    afterEach(() => {
        vi.restoreAllMocks();
        vi.resetAllMocks();
    });

    it('should extract data using default path when no custom path is provided', () => {
        const mockEntityData = {
            globalTags: {
                tags: [{ tag: { name: 'test-tag' } }],
            },
        };

        mockUseEntityData.mockReturnValue({
            entityData: mockEntityData,
            entityType: 'DATASET',
            urn: 'test-urn',
        } as any);

        const { result } = renderHook(() =>
            useEntityDataExtractor({
                defaultPath: 'globalTags',
                arrayProperty: 'tags',
            }),
        );

        expect(result.current.data).toEqual(mockEntityData.globalTags);
        expect(result.current.isEmpty).toBe(false);
    });

    it('should extract data using custom path when provided', () => {
        const mockEntityData = {
            customField: {
                tags: [{ tag: { name: 'custom-tag' } }],
            },
            globalTags: {
                tags: [{ tag: { name: 'default-tag' } }],
            },
        };

        mockUseEntityData.mockReturnValue({
            entityData: mockEntityData,
            entityType: 'DATASET',
            urn: 'test-urn',
        } as any);

        const { result } = renderHook(() =>
            useEntityDataExtractor({
                customPath: 'customField',
                defaultPath: 'globalTags',
                arrayProperty: 'tags',
            }),
        );

        expect(result.current.data).toEqual(mockEntityData.customField);
        expect(result.current.isEmpty).toBe(false);
    });

    it('should return isEmpty as true when array is empty', () => {
        const mockEntityData = {
            globalTags: {
                tags: [],
            },
        };

        mockUseEntityData.mockReturnValue({
            entityData: mockEntityData,
            entityType: 'DATASET',
            urn: 'test-urn',
        } as any);

        const { result } = renderHook(() =>
            useEntityDataExtractor({
                defaultPath: 'globalTags',
                arrayProperty: 'tags',
            }),
        );

        expect(result.current.data).toEqual(mockEntityData.globalTags);
        expect(result.current.isEmpty).toBe(true);
    });

    it('should return isEmpty as true when data is undefined', () => {
        const mockEntityData = {};

        mockUseEntityData.mockReturnValue({
            entityData: mockEntityData,
            entityType: 'DATASET',
            urn: 'test-urn',
        } as any);

        const { result } = renderHook(() =>
            useEntityDataExtractor({
                defaultPath: 'globalTags',
                arrayProperty: 'tags',
            }),
        );

        expect(result.current.data).toBeUndefined();
        expect(result.current.isEmpty).toBe(true);
    });

    it('should work with glossary terms', () => {
        const mockEntityData = {
            glossaryTerms: {
                terms: [{ term: { name: 'test-term' } }],
            },
        };

        mockUseEntityData.mockReturnValue({
            entityData: mockEntityData,
            entityType: 'DATASET',
            urn: 'test-urn',
        } as any);

        const { result } = renderHook(() =>
            useEntityDataExtractor({
                defaultPath: 'glossaryTerms',
                arrayProperty: 'terms',
            }),
        );

        expect(result.current.data).toEqual(mockEntityData.glossaryTerms);
        expect(result.current.isEmpty).toBe(false);
    });
});
