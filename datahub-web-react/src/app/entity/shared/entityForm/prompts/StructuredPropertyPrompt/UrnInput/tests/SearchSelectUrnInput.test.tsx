import { MockedProvider } from '@apollo/client/testing';
import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { EntityType } from '../../../../../../../../types.generated';
import TestPageContainer from '../../../../../../../../utils/test-utils/TestPageContainer';
import { EntityAndType } from '../../../../../types';
import { SearchSelectUrnInput } from '../SearchSelectUrnInput';

// Mock the useHydratedEntityMap hook
vi.mock('@src/app/entityV2/shared/tabs/Properties/useHydratedEntityMap', () => ({
    useHydratedEntityMap: (urns: string[]) => {
        const map: { [key: string]: any } = {};
        urns?.forEach((urn) => {
            // Simple mock entity structure
            map[urn] = {
                urn,
                type: EntityType.Dataset, // Assuming dataset for simplicity
                properties: { name: `Entity ${urn.split(':').pop()}` },
                platform: {
                    urn: 'urn:li:dataPlatform:test',
                    name: 'test-platform',
                    properties: {
                        displayName: 'Test Platform',
                        logoUrl: '',
                    },
                },
                // Add other necessary fields if EntityLink mock needs them
            };
        });
        return map;
    },
}));

// Mock SearchSelect component - capture the setSelectedEntities callback
const mockSetSelectedEntities = vi.fn(); // Spy will now store the *callback itself* when called
vi.mock('../../../../../../../entityV2/shared/components/styled/search/SearchSelect', () => ({
    SearchSelect: (props: { setSelectedEntities: (entities: EntityAndType[]) => void }) => {
        // Call the spy with the actual callback function when the mock renders
        if (props.setSelectedEntities) {
            // Check if the prop exists before calling
            mockSetSelectedEntities(props.setSelectedEntities);
        }
        return <div data-testid="mock-search-select">Mock SearchSelect</div>;
    },
}));

// Mock EntityLink component
vi.mock('@src/app/homeV2/reference/sections/EntityLink', () => ({
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    EntityLink: ({ entity }: { entity: any }) => (
        <div data-testid={`entity-link-${entity.urn}`}>{entity.properties?.name || entity.urn}</div>
    ),
}));

// Mock Icon component used for removal
vi.mock('@src/alchemy-components', async () => {
    // Import the actual module
    const actual = await vi.importActual<typeof import('@src/alchemy-components')>('@src/alchemy-components');

    return {
        // Spread the actual module exports
        ...actual,
        // Override specific components with mocks
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        Icon: (props: { icon: string; onClick?: () => void; [key: string]: any }) => (
            <button data-testid={`mock-icon-${props.icon.toLowerCase()}`} onClick={props.onClick}>
                {props.icon}
            </button>
        ),
        Tooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
        Button: ({ children, onClick }: { children: React.ReactNode; onClick?: () => void }) => (
            <button onClick={onClick}>{children}</button>
        ),
        // No need to mock colors and typography anymore, we use the actual ones
    };
});

describe('SearchSelectUrnInput', () => {
    const mockUpdateSelectedValues = vi.fn();

    beforeEach(() => {
        mockUpdateSelectedValues.mockClear();
        mockSetSelectedEntities.mockClear();
    });

    it('renders correctly with no initial selection', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <SearchSelectUrnInput
                        allowedEntityTypes={[EntityType.Dataset]}
                        isMultiple
                        selectedValues={[]}
                        updateSelectedValues={mockUpdateSelectedValues}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Search Values')).toBeInTheDocument();
        expect(screen.getByTestId('mock-search-select')).toBeInTheDocument();
        expect(screen.getByText('Selected Values')).toBeInTheDocument();
        expect(screen.getByText('No values selected')).toBeInTheDocument();
    });

    it('renders with initial selected values', () => {
        const initialUrns = ['urn:li:dataset:1', 'urn:li:dataset:2'];
        render(
            <MockedProvider>
                <TestPageContainer>
                    <SearchSelectUrnInput
                        allowedEntityTypes={[EntityType.Dataset]}
                        isMultiple
                        selectedValues={initialUrns}
                        updateSelectedValues={mockUpdateSelectedValues}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Selected Values')).toBeInTheDocument();
        expect(screen.getByTestId('entity-link-urn:li:dataset:1')).toHaveTextContent('Entity 1');
        expect(screen.getByTestId('entity-link-urn:li:dataset:2')).toHaveTextContent('Entity 2');
        expect(screen.queryByText('No values selected')).not.toBeInTheDocument();
    });

    it('calls updateSelectedValues when a value is removed', () => {
        const initialUrns = ['urn:li:dataset:1', 'urn:li:dataset:2'];
        render(
            <MockedProvider>
                <TestPageContainer>
                    <SearchSelectUrnInput
                        allowedEntityTypes={[EntityType.Dataset]}
                        isMultiple
                        selectedValues={initialUrns}
                        updateSelectedValues={mockUpdateSelectedValues}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Find the remove button for the first entity
        const removeButton = screen.getAllByTestId('mock-icon-x')[0]; // Assuming order matches initialUrns
        fireEvent.click(removeButton);

        // Check if updateSelectedValues was called with the remaining URN
        // It gets called twice: once on initial load (useEffect) and once on removal
        expect(mockUpdateSelectedValues).toHaveBeenCalledTimes(2);
        expect(mockUpdateSelectedValues).toHaveBeenLastCalledWith(['urn:li:dataset:2']);

        // Check if the removed entity is no longer rendered
        expect(screen.queryByTestId('entity-link-urn:li:dataset:1')).not.toBeInTheDocument();
        expect(screen.getByTestId('entity-link-urn:li:dataset:2')).toBeInTheDocument();
    });

    it('calls updateSelectedValues when SearchSelect updates selection', () => {
        const initialUrns: string[] = [];
        const newEntity: EntityAndType = { urn: 'urn:li:dataset:3', type: EntityType.Dataset };
        render(
            <MockedProvider>
                <TestPageContainer>
                    <SearchSelectUrnInput
                        allowedEntityTypes={[EntityType.Dataset]}
                        isMultiple
                        selectedValues={initialUrns}
                        updateSelectedValues={mockUpdateSelectedValues}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Ensure the SearchSelect mock rendered and called our spy with the callback
        expect(mockSetSelectedEntities).toHaveBeenCalled();

        // Get the actual callback function from the spy's arguments
        const capturedSetSelectedEntities = mockSetSelectedEntities.mock.calls[0][0];
        expect(capturedSetSelectedEntities).toBeInstanceOf(Function); // Verify it's a function

        // Simulate SearchSelect calling its setSelectedEntities prop within act
        act(() => {
            capturedSetSelectedEntities([newEntity]);
        });

        // Check if updateSelectedValues was called with the new URN
        // It gets called once on initial load (useEffect) and once when the selection changes
        expect(mockUpdateSelectedValues).toHaveBeenCalledTimes(2);
        expect(mockUpdateSelectedValues).toHaveBeenLastCalledWith([newEntity.urn]);

        // Check if the new entity is rendered
        expect(screen.getByTestId('entity-link-urn:li:dataset:3')).toHaveTextContent('Entity 3');
    });
});
