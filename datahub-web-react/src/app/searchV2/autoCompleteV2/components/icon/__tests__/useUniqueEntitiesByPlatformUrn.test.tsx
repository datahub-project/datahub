import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import useUniqueEntitiesByPlatformUrn from '@app/searchV2/autoCompleteV2/components/icon/useUniqueEntitiesByPlatformUrn';
import { CorpGroup, Dataset, EntityType, FabricType } from '@src/types.generated';
import TestPageContainer from '@src/utils/test-utils/TestPageContainer';

function generateSampleEntity(urn: string, platformUrn: string): Dataset {
    return {
        urn,
        type: EntityType.Dataset,
        name: 'Test',
        platform: {
            name: 'Test',
            type: EntityType.DataPlatform,
            urn: platformUrn,
        },
        origin: FabricType.Test,
    };
}

function generateSampleEntityWithoutPlatform(urn: string): CorpGroup {
    return {
        urn,
        type: EntityType.Dataset,
        name: 'Test',
    };
}

describe('useUniqueEntitiesByPlatformUrn', () => {
    const wrapper = ({ children }) => (
        <MockedProvider>
            <TestPageContainer>{children}</TestPageContainer>
        </MockedProvider>
    );

    it('should return entities with unique urns of platforms', () => {
        const response = renderHook(
            () =>
                useUniqueEntitiesByPlatformUrn([
                    generateSampleEntity('dataset1', 'platform1'),
                    generateSampleEntity('dataset2', 'platform2'),
                    generateSampleEntity('dataset3', 'platform2'),
                ]),
            { wrapper },
        ).result.current;

        expect(response).toStrictEqual([
            {
                urn: 'dataset1',
                type: 'DATASET',
                name: 'Test',
                platform: { name: 'Test', type: 'DATA_PLATFORM', urn: 'platform1' },
                origin: 'TEST',
            },
            {
                urn: 'dataset2',
                type: 'DATASET',
                name: 'Test',
                platform: { name: 'Test', type: 'DATA_PLATFORM', urn: 'platform2' },
                origin: 'TEST',
            },
        ]);
    });

    it("should handle entities without platform's urn", () => {
        const response = renderHook(
            () =>
                useUniqueEntitiesByPlatformUrn([
                    generateSampleEntity('dataset1', 'platform1'),
                    generateSampleEntity('dataset2', 'platform2'),
                    generateSampleEntityWithoutPlatform('dataset3'),
                ]),
            { wrapper },
        ).result.current;

        expect(response).toStrictEqual([
            {
                urn: 'dataset1',
                type: 'DATASET',
                name: 'Test',
                platform: { name: 'Test', type: 'DATA_PLATFORM', urn: 'platform1' },
                origin: 'TEST',
            },
            {
                urn: 'dataset2',
                type: 'DATASET',
                name: 'Test',
                platform: { name: 'Test', type: 'DATA_PLATFORM', urn: 'platform2' },
                origin: 'TEST',
            },
        ]);
    });

    it('should handle empty array', () => {
        const response = renderHook(() => useUniqueEntitiesByPlatformUrn([]), { wrapper }).result.current;

        expect(response).toStrictEqual([]);
    });

    it('should handle undefined input', () => {
        const response = renderHook(() => useUniqueEntitiesByPlatformUrn(undefined), { wrapper }).result.current;

        expect(response).toStrictEqual([]);
    });
});
