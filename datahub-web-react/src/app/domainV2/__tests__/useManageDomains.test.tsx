import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import { DomainsContext, DomainsContextType, UpdatedDomain } from '@app/domainV2/DomainsContext';
import useManageDomains from '@app/domainV2/useManageDomains';

import { ListDomainFragment } from '@graphql/domain.generated';
import { EntityType } from '@types';

// Mock domain data for testing
const createMockDomain = (urn: string, name: string, parentDomain?: string): UpdatedDomain => ({
    urn,
    id: urn,
    type: EntityType.Domain,
    properties: {
        name,
        description: `Description for ${name}`,
    },
    ownership: null,
    entities: null,
    children: { total: 0 },
    dataProducts: null,
    parentDomains: null,
    displayProperties: null,
    parentDomain,
});

// Helper to create context wrapper for testing
const createContextWrapper = (contextValue: Partial<DomainsContextType>) => {
    const defaultContextValue: DomainsContextType = {
        entityData: null,
        setEntityData: vi.fn(),
        newDomain: null,
        setNewDomain: vi.fn(),
        deletedDomain: null,
        setDeletedDomain: vi.fn(),
        updatedDomain: null,
        setUpdatedDomain: vi.fn(),
        ...contextValue,
    };

    return ({ children }: { children: React.ReactNode }) => (
        <DomainsContext.Provider value={defaultContextValue}>{children}</DomainsContext.Provider>
    );
};

describe('useManageDomains', () => {
    let mockSetData: ReturnType<typeof vi.fn>;
    let mockSetDataUrnsSet: ReturnType<typeof vi.fn>;
    let mockSetNewDomain: ReturnType<typeof vi.fn>;
    let mockSetDeletedDomain: ReturnType<typeof vi.fn>;
    let mockSetUpdatedDomain: ReturnType<typeof vi.fn>;

    const domain1 = createMockDomain('urn:li:domain:1', 'Domain 1');
    const domain2 = createMockDomain('urn:li:domain:2', 'Domain 2');

    beforeEach(() => {
        mockSetData = vi.fn();
        mockSetDataUrnsSet = vi.fn();
        mockSetNewDomain = vi.fn();
        mockSetDeletedDomain = vi.fn();
        mockSetUpdatedDomain = vi.fn();
    });

    describe('Adding new domain', () => {
        it('should add new domain to the list when parent domain matches', () => {
            const newDomain = createMockDomain('urn:li:domain:new', 'New Domain', 'urn:li:domain:1');
            const dataUrnsSet = new Set(['urn:li:domain:1', 'urn:li:domain:2']);

            const wrapper = createContextWrapper({
                newDomain,
                setNewDomain: mockSetNewDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: 'urn:li:domain:1',
                    }),
                { wrapper },
            );

            expect(mockSetData).toHaveBeenCalledWith(expect.any(Function));
            expect(mockSetDataUrnsSet).toHaveBeenCalledWith(expect.any(Function));
            expect(mockSetNewDomain).toHaveBeenCalledWith(null);

            // Test the data setter function
            const dataSetterFn = mockSetData.mock.calls[0][0];
            const newData = dataSetterFn([domain1, domain2]);
            expect(newData).toEqual([newDomain, domain1, domain2]);

            // Test the dataUrnsSet setter function
            const urnsSetterFn = mockSetDataUrnsSet.mock.calls[0][0];
            const newUrnsSet = urnsSetterFn(dataUrnsSet);
            expect(newUrnsSet).toEqual(new Set(['urn:li:domain:1', 'urn:li:domain:2', 'urn:li:domain:new']));
        });

        it('should not add new domain when parent domain does not match', () => {
            const newDomain = createMockDomain('urn:li:domain:new', 'New Domain', 'urn:li:domain:other');
            const dataUrnsSet = new Set(['urn:li:domain:1', 'urn:li:domain:2']);

            const wrapper = createContextWrapper({
                newDomain,
                setNewDomain: mockSetNewDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: 'urn:li:domain:1',
                    }),
                { wrapper },
            );

            expect(mockSetData).not.toHaveBeenCalled();
            expect(mockSetDataUrnsSet).not.toHaveBeenCalled();
            expect(mockSetNewDomain).not.toHaveBeenCalled();
        });

        it('should increase parent domain children count when new domain is added to a parent in the list', () => {
            const parentDomain = createMockDomain('urn:li:domain:parent', 'Parent Domain');
            parentDomain.children = { total: 5 };
            const newDomain = createMockDomain('urn:li:domain:new', 'New Domain', 'urn:li:domain:parent');
            const dataUrnsSet = new Set(['urn:li:domain:parent', 'urn:li:domain:2']);

            const wrapper = createContextWrapper({
                newDomain,
                setNewDomain: mockSetNewDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: undefined, // We're at root level
                    }),
                { wrapper },
            );

            // Should call setData twice - once for parent count update
            expect(mockSetData).toHaveBeenCalledTimes(1);

            const dataSetterFn = mockSetData.mock.calls[0][0];
            const currentData = [parentDomain, domain2];
            const updatedData = dataSetterFn(currentData);

            const updatedParent = updatedData.find((d: ListDomainFragment) => d.urn === 'urn:li:domain:parent');
            expect(updatedParent?.children?.total).toBe(6);
        });

        it('should handle adding new root domain (no parent)', () => {
            const newRootDomain = createMockDomain('urn:li:domain:newroot', 'New Root Domain');
            // No parentDomain property for root domains
            delete newRootDomain.parentDomain;
            const dataUrnsSet = new Set(['urn:li:domain:1', 'urn:li:domain:2']);

            const wrapper = createContextWrapper({
                newDomain: newRootDomain,
                setNewDomain: mockSetNewDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: undefined, // Root level
                    }),
                { wrapper },
            );

            expect(mockSetData).toHaveBeenCalledWith(expect.any(Function));
            expect(mockSetDataUrnsSet).toHaveBeenCalledWith(expect.any(Function));
            expect(mockSetNewDomain).toHaveBeenCalledWith(null);
        });
    });

    describe('Deleting domain', () => {
        it('should remove domain from the list when domain exists in dataUrnsSet', () => {
            const deletedDomain = createMockDomain('urn:li:domain:2', 'Domain 2');
            const dataUrnsSet = new Set(['urn:li:domain:1', 'urn:li:domain:2']);

            const wrapper = createContextWrapper({
                deletedDomain,
                setDeletedDomain: mockSetDeletedDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: undefined,
                    }),
                { wrapper },
            );

            expect(mockSetData).toHaveBeenCalledWith(expect.any(Function));
            expect(mockSetDeletedDomain).toHaveBeenCalledWith(null);

            // Test the data filter function
            const dataSetterFn = mockSetData.mock.calls[0][0];
            const filteredData = dataSetterFn([domain1, domain2]);
            expect(filteredData).toEqual([domain1]);
        });

        it('should not remove domain when domain does not exist in dataUrnsSet', () => {
            const deletedDomain = createMockDomain('urn:li:domain:other', 'Other Domain');
            const dataUrnsSet = new Set(['urn:li:domain:1', 'urn:li:domain:2']);

            const wrapper = createContextWrapper({
                deletedDomain,
                setDeletedDomain: mockSetDeletedDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: undefined,
                    }),
                { wrapper },
            );

            expect(mockSetData).not.toHaveBeenCalled();
            expect(mockSetDeletedDomain).not.toHaveBeenCalled();
        });

        it('should decrease parent domain children count when domain is deleted', () => {
            const parentDomain = createMockDomain('urn:li:domain:parent', 'Parent Domain');
            parentDomain.children = { total: 5 };
            const deletedDomain = createMockDomain('urn:li:domain:child', 'Child Domain', 'urn:li:domain:parent');
            const dataUrnsSet = new Set(['urn:li:domain:parent', 'urn:li:domain:child']);

            const wrapper = createContextWrapper({
                deletedDomain,
                setDeletedDomain: mockSetDeletedDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: undefined,
                    }),
                { wrapper },
            );

            expect(mockSetData).toHaveBeenCalledTimes(2); // Once for deletion, once for parent count update

            // Test parent count update
            const parentUpdateFn = mockSetData.mock.calls[1][0];
            const currentData = [parentDomain, deletedDomain];
            const updatedData = parentUpdateFn(currentData);

            const updatedParent = updatedData.find((d: ListDomainFragment) => d.urn === 'urn:li:domain:parent');
            expect(updatedParent?.children?.total).toBe(4);
        });

        it('should not decrease parent count below 0', () => {
            const parentDomain = createMockDomain('urn:li:domain:parent', 'Parent Domain');
            parentDomain.children = { total: 0 };
            const deletedDomain = createMockDomain('urn:li:domain:child', 'Child Domain', 'urn:li:domain:parent');
            const dataUrnsSet = new Set(['urn:li:domain:parent', 'urn:li:domain:child']);

            const wrapper = createContextWrapper({
                deletedDomain,
                setDeletedDomain: mockSetDeletedDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: undefined,
                    }),
                { wrapper },
            );

            const parentUpdateFn = mockSetData.mock.calls[1][0];
            const currentData = [parentDomain];
            const updatedData = parentUpdateFn(currentData);

            const updatedParent = updatedData.find((d: ListDomainFragment) => d.urn === 'urn:li:domain:parent');
            expect(updatedParent?.children?.total).toBe(0);
        });
    });

    describe('Updating domain', () => {
        it('should update domain in the list when domain exists in dataUrnsSet', () => {
            const updatedDomain = createMockDomain('urn:li:domain:2', 'Updated Domain 2');
            updatedDomain.properties!.description = 'Updated description';
            const dataUrnsSet = new Set(['urn:li:domain:1', 'urn:li:domain:2']);

            const wrapper = createContextWrapper({
                updatedDomain,
                setUpdatedDomain: mockSetUpdatedDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: undefined,
                    }),
                { wrapper },
            );

            expect(mockSetData).toHaveBeenCalledWith(expect.any(Function));
            expect(mockSetUpdatedDomain).toHaveBeenCalledWith(null);

            // Test the data update function
            const dataSetterFn = mockSetData.mock.calls[0][0];
            const updatedData = dataSetterFn([domain1, domain2]);

            expect(updatedData[1]).toEqual(
                expect.objectContaining({
                    urn: 'urn:li:domain:2',
                    properties: expect.objectContaining({
                        name: 'Updated Domain 2',
                        description: 'Updated description',
                    }),
                }),
            );
        });

        it('should not update domain when domain does not exist in dataUrnsSet', () => {
            const updatedDomain = createMockDomain('urn:li:domain:other', 'Other Domain');
            const dataUrnsSet = new Set(['urn:li:domain:1', 'urn:li:domain:2']);

            const wrapper = createContextWrapper({
                updatedDomain,
                setUpdatedDomain: mockSetUpdatedDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: undefined,
                    }),
                { wrapper },
            );

            expect(mockSetData).not.toHaveBeenCalled();
            expect(mockSetUpdatedDomain).not.toHaveBeenCalled();
        });
    });

    describe('Effect dependencies', () => {
        it('should react to changes in context values', () => {
            const newDomain = createMockDomain('urn:li:domain:new', 'New Domain');
            const dataUrnsSet = new Set(['urn:li:domain:1']);

            const wrapper = createContextWrapper({
                newDomain,
                setNewDomain: mockSetNewDomain,
                deletedDomain: null,
                setDeletedDomain: mockSetDeletedDomain,
                updatedDomain: null,
                setUpdatedDomain: mockSetUpdatedDomain,
            });

            renderHook(
                () =>
                    useManageDomains({
                        dataUrnsSet,
                        setDataUrnsSet: mockSetDataUrnsSet,
                        setData: mockSetData,
                        parentDomain: undefined,
                    }),
                { wrapper },
            );

            // Should be called because newDomain is provided and parentDomain matches (both undefined)
            expect(mockSetData).toHaveBeenCalled();
            expect(mockSetDataUrnsSet).toHaveBeenCalled();
            expect(mockSetNewDomain).toHaveBeenCalledWith(null);
        });
    });
});
