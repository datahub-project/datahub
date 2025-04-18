import React from 'react';
import { MockedProvider } from '@apollo/client/testing';
import EntityContext from '@src/app/entity/shared/EntityContext';
import useEntityDataForForm from '@src/app/entity/shared/entityForm/useEntityDataForForm';
import { container1, dataset3, mocks } from '@src/Mocks';
import { EntityType } from '@src/types.generated';
import TestPageContainer, { getTestEntityRegistry } from '@src/utils/test-utils/TestPageContainer';
import { renderHook } from '@testing-library/react-hooks';
import { fail } from 'assert';

describe('useEntityDataForForm', () => {
    const entityRegistry = getTestEntityRegistry();

    it('should return the correct values when there is no selected entity', () => {
        const wrapper = ({ children }) => (
            <MockedProvider>
                <TestPageContainer>{children}</TestPageContainer>
            </MockedProvider>
        );

        const { result } = renderHook(() => useEntityDataForForm({}), { wrapper });
        const { selectedEntityData, isOnEntityProfilePage } = result.current;

        expect(selectedEntityData).toBe(null);
        expect(isOnEntityProfilePage).toBe(false);
    });

    it('should return the correct values when there is a selected entity and we are on its profile page', () => {
        const testEntityData = { urn: 'urn:li:dataset:123', type: EntityType.Dataset, properties: { name: 'test123' } };
        const wrapper = ({ children }) => (
            <MockedProvider>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: testEntityData,
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        {children}
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>
        );

        const { result } = renderHook(() => useEntityDataForForm({ selectedEntity: testEntityData }), { wrapper });
        const { selectedEntityData, isOnEntityProfilePage } = result.current;

        expect(selectedEntityData).toMatchObject(testEntityData);
        expect(isOnEntityProfilePage).toBe(true);
    });

    it('should return the correct values when there is a selected entity and we are NOT on its profile page', async () => {
        const testEntityData = { urn: 'urn:li:dataset:3', type: EntityType.Dataset, properties: { name: 'test123' } };
        const wrapper = ({ children }) => (
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:456',
                            entityType: EntityType.Dataset,
                            entityData: {
                                urn: 'urn:li:dataset:456',
                                type: EntityType.Dataset,
                                properties: { name: 'test456' },
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: false,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        {children}
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>
        );

        const { result, waitForNextUpdate } = renderHook(
            () => useEntityDataForForm({ selectedEntity: testEntityData }),
            { wrapper },
        );
        await waitForNextUpdate();

        const { selectedEntityData, isOnEntityProfilePage } = result.current;

        const expectedData = entityRegistry.getGenericEntityProperties(dataset3.type, dataset3);
        if (expectedData) {
            expect(selectedEntityData).toMatchObject(expectedData);
            expect(isOnEntityProfilePage).toBe(false);
        } else {
            fail('expectedData is null when it should not be');
        }
    });

    it('should return the correct values when there is a selected entity and we are not on any profile page', async () => {
        const testEntityData = { urn: 'urn:li:dataset:3', type: EntityType.Dataset, properties: { name: 'test123' } };
        const wrapper = ({ children }) => (
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>{children}</TestPageContainer>
            </MockedProvider>
        );

        const { result, waitForNextUpdate } = renderHook(
            () => useEntityDataForForm({ selectedEntity: testEntityData }),
            { wrapper },
        );
        await waitForNextUpdate();

        const { selectedEntityData, isOnEntityProfilePage } = result.current;

        const expectedData = entityRegistry.getGenericEntityProperties(dataset3.type, dataset3);
        if (expectedData) {
            expect(selectedEntityData).toMatchObject(expectedData);
            expect(isOnEntityProfilePage).toBe(false);
        } else {
            fail('expectedData is null when it should not be');
        }
    });

    it('should return the correct values when there is a selected entity and we are not on any profile page for containers', async () => {
        const testEntityData = {
            urn: 'urn:li:container:DATABASE',
            type: EntityType.Container,
            properties: { name: 'DATABASE' },
        };
        const wrapper = ({ children }) => (
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>{children}</TestPageContainer>
            </MockedProvider>
        );

        const { result, waitForNextUpdate } = renderHook(
            () => useEntityDataForForm({ selectedEntity: testEntityData }),
            { wrapper },
        );
        await waitForNextUpdate();

        const { selectedEntityData, isOnEntityProfilePage } = result.current;

        const expectedData = entityRegistry.getGenericEntityProperties(container1.type, container1);
        if (expectedData) {
            expect(selectedEntityData).toMatchObject(expectedData);
            expect(isOnEntityProfilePage).toBe(false);
        } else {
            fail('expectedData is null when it should not be');
        }
    });
});
