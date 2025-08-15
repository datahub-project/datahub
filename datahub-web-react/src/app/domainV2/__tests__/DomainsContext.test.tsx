import { fireEvent, render } from '@testing-library/react';
import React, { useState } from 'react';

import { DomainsContext, UpdatedDomain, useDomainsContext } from '@app/domainV2/DomainsContext';
import { GenericEntityProperties } from '@app/entity/shared/types';

import { EntityType } from '@types';

// Mock domain for testing
const createMockDomain = (urn: string, name: string): UpdatedDomain => ({
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
});

// Test component that uses the context
const TestComponent = () => {
    const {
        entityData,
        setEntityData,
        newDomain,
        setNewDomain,
        deletedDomain,
        setDeletedDomain,
        updatedDomain,
        setUpdatedDomain,
    } = useDomainsContext();

    return (
        <div>
            <div data-testid="entity-data">{entityData ? JSON.stringify(entityData) : 'null'}</div>
            <div data-testid="new-domain">{newDomain ? newDomain.urn : 'null'}</div>
            <div data-testid="deleted-domain">{deletedDomain ? deletedDomain.urn : 'null'}</div>
            <div data-testid="updated-domain">{updatedDomain ? updatedDomain.urn : 'null'}</div>

            <button
                type="button"
                data-testid="set-entity-data"
                onClick={() => setEntityData({ urn: 'test-urn', type: EntityType.Domain })}
            >
                Set Entity Data
            </button>
            <button
                type="button"
                data-testid="set-new-domain"
                onClick={() => setNewDomain(createMockDomain('urn:li:domain:new', 'New Domain'))}
            >
                Set New Domain
            </button>
            <button
                type="button"
                data-testid="set-deleted-domain"
                onClick={() => setDeletedDomain(createMockDomain('urn:li:domain:deleted', 'Deleted Domain'))}
            >
                Set Deleted Domain
            </button>
            <button
                type="button"
                data-testid="set-updated-domain"
                onClick={() => setUpdatedDomain(createMockDomain('urn:li:domain:updated', 'Updated Domain'))}
            >
                Set Updated Domain
            </button>
            <button
                type="button"
                data-testid="clear-all"
                onClick={() => {
                    setEntityData(null);
                    setNewDomain(null);
                    setDeletedDomain(null);
                    setUpdatedDomain(null);
                }}
            >
                Clear All
            </button>
        </div>
    );
};

// Test provider component that manages context state
const TestProvider = ({ children }: { children: React.ReactNode }) => {
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [newDomain, setNewDomain] = useState<UpdatedDomain | null>(null);
    const [deletedDomain, setDeletedDomain] = useState<UpdatedDomain | null>(null);
    const [updatedDomain, setUpdatedDomain] = useState<UpdatedDomain | null>(null);

    return (
        <DomainsContext.Provider
            value={{
                entityData,
                setEntityData,
                newDomain,
                setNewDomain,
                deletedDomain,
                setDeletedDomain,
                updatedDomain,
                setUpdatedDomain,
            }}
        >
            {children}
        </DomainsContext.Provider>
    );
};

describe('DomainsContext', () => {
    describe('Context Provider', () => {
        it('should provide default values when no provider is used', () => {
            const { getByTestId } = render(<TestComponent />);

            expect(getByTestId('entity-data')).toHaveTextContent('null');
            expect(getByTestId('new-domain')).toHaveTextContent('null');
            expect(getByTestId('deleted-domain')).toHaveTextContent('null');
            expect(getByTestId('updated-domain')).toHaveTextContent('null');
        });

        it('should allow setting and getting entityData', () => {
            const { getByTestId } = render(
                <TestProvider>
                    <TestComponent />
                </TestProvider>,
            );

            expect(getByTestId('entity-data')).toHaveTextContent('null');

            fireEvent.click(getByTestId('set-entity-data'));

            expect(getByTestId('entity-data')).toHaveTextContent(
                JSON.stringify({ urn: 'test-urn', type: EntityType.Domain }),
            );
        });

        it('should allow setting and getting newDomain', () => {
            const { getByTestId } = render(
                <TestProvider>
                    <TestComponent />
                </TestProvider>,
            );

            expect(getByTestId('new-domain')).toHaveTextContent('null');

            fireEvent.click(getByTestId('set-new-domain'));

            expect(getByTestId('new-domain')).toHaveTextContent('urn:li:domain:new');
        });

        it('should allow setting and getting deletedDomain', () => {
            const { getByTestId } = render(
                <TestProvider>
                    <TestComponent />
                </TestProvider>,
            );

            expect(getByTestId('deleted-domain')).toHaveTextContent('null');

            fireEvent.click(getByTestId('set-deleted-domain'));

            expect(getByTestId('deleted-domain')).toHaveTextContent('urn:li:domain:deleted');
        });

        it('should allow setting and getting updatedDomain', () => {
            const { getByTestId } = render(
                <TestProvider>
                    <TestComponent />
                </TestProvider>,
            );

            expect(getByTestId('updated-domain')).toHaveTextContent('null');

            fireEvent.click(getByTestId('set-updated-domain'));

            expect(getByTestId('updated-domain')).toHaveTextContent('urn:li:domain:updated');
        });

        it('should allow clearing all context values', () => {
            const { getByTestId } = render(
                <TestProvider>
                    <TestComponent />
                </TestProvider>,
            );

            // Set all values first
            fireEvent.click(getByTestId('set-entity-data'));
            fireEvent.click(getByTestId('set-new-domain'));
            fireEvent.click(getByTestId('set-deleted-domain'));
            fireEvent.click(getByTestId('set-updated-domain'));

            // Verify they are set
            expect(getByTestId('entity-data')).not.toHaveTextContent('null');
            expect(getByTestId('new-domain')).not.toHaveTextContent('null');
            expect(getByTestId('deleted-domain')).not.toHaveTextContent('null');
            expect(getByTestId('updated-domain')).not.toHaveTextContent('null');

            // Clear all
            fireEvent.click(getByTestId('clear-all'));

            // Verify they are cleared
            expect(getByTestId('entity-data')).toHaveTextContent('null');
            expect(getByTestId('new-domain')).toHaveTextContent('null');
            expect(getByTestId('deleted-domain')).toHaveTextContent('null');
            expect(getByTestId('updated-domain')).toHaveTextContent('null');
        });
    });

    describe('useDomainsContext hook', () => {
        it('should return all context values and setters', () => {
            let contextValues: any;

            const TestComponent2 = () => {
                contextValues = useDomainsContext();
                return <div />;
            };

            render(
                <TestProvider>
                    <TestComponent2 />
                </TestProvider>,
            );

            expect(contextValues).toHaveProperty('entityData');
            expect(contextValues).toHaveProperty('setEntityData');
            expect(contextValues).toHaveProperty('newDomain');
            expect(contextValues).toHaveProperty('setNewDomain');
            expect(contextValues).toHaveProperty('deletedDomain');
            expect(contextValues).toHaveProperty('setDeletedDomain');
            expect(contextValues).toHaveProperty('updatedDomain');
            expect(contextValues).toHaveProperty('setUpdatedDomain');

            // Verify setters are functions
            expect(typeof contextValues.setEntityData).toBe('function');
            expect(typeof contextValues.setNewDomain).toBe('function');
            expect(typeof contextValues.setDeletedDomain).toBe('function');
            expect(typeof contextValues.setUpdatedDomain).toBe('function');
        });

        it('should maintain separate state for different values', () => {
            const { getByTestId } = render(
                <TestProvider>
                    <TestComponent />
                </TestProvider>,
            );

            // Set new domain
            fireEvent.click(getByTestId('set-new-domain'));
            expect(getByTestId('new-domain')).toHaveTextContent('urn:li:domain:new');
            expect(getByTestId('deleted-domain')).toHaveTextContent('null');
            expect(getByTestId('updated-domain')).toHaveTextContent('null');

            // Set deleted domain
            fireEvent.click(getByTestId('set-deleted-domain'));
            expect(getByTestId('new-domain')).toHaveTextContent('urn:li:domain:new');
            expect(getByTestId('deleted-domain')).toHaveTextContent('urn:li:domain:deleted');
            expect(getByTestId('updated-domain')).toHaveTextContent('null');

            // Set updated domain
            fireEvent.click(getByTestId('set-updated-domain'));
            expect(getByTestId('new-domain')).toHaveTextContent('urn:li:domain:new');
            expect(getByTestId('deleted-domain')).toHaveTextContent('urn:li:domain:deleted');
            expect(getByTestId('updated-domain')).toHaveTextContent('urn:li:domain:updated');
        });
    });

    describe('UpdatedDomain type', () => {
        it('should work with domains that have parentDomain property', () => {
            const domainWithParent = createMockDomain('urn:li:domain:child', 'Child Domain');
            domainWithParent.parentDomain = 'urn:li:domain:parent';

            const TestComponentWithParent = () => {
                const { newDomain, setNewDomain } = useDomainsContext();

                return (
                    <div>
                        <div data-testid="domain-parent">{newDomain?.parentDomain || 'null'}</div>
                        <button
                            type="button"
                            data-testid="set-domain-with-parent"
                            onClick={() => setNewDomain(domainWithParent)}
                        >
                            Set Domain With Parent
                        </button>
                    </div>
                );
            };

            const { getByTestId } = render(
                <TestProvider>
                    <TestComponentWithParent />
                </TestProvider>,
            );

            expect(getByTestId('domain-parent')).toHaveTextContent('null');

            fireEvent.click(getByTestId('set-domain-with-parent'));

            expect(getByTestId('domain-parent')).toHaveTextContent('urn:li:domain:parent');
        });
    });
});
