import React from 'react';
import { render, screen } from '@testing-library/react';
import { act } from 'react-dom/test-utils';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import useAvailableAssetProperties from '@app/entityV2/summary/properties/hooks/useAvailableAssetProperties';
import useInitialAssetProperties from '@app/entityV2/summary/properties/hooks/useInitialAssetProperties';
import { PropertyType } from '@app/entityV2/summary/properties/types';
import { EntityType } from '@types';
import AssetPropertiesProvider from '@app/entityV2/summary/properties/context/AssetPropertiesProvider';
import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';

vi.mock('@app/entity/shared/EntityContext');
vi.mock('@app/entityV2/summary/properties/hooks/useInitialAssetProperties');
vi.mock('@app/entityV2/summary/properties/hooks/useAvailableAssetProperties');

const useEntityContextMock = vi.mocked(useEntityContext);
const useInitialAssetPropertiesMock = vi.mocked(useInitialAssetProperties);
const useAvailableAssetPropertiesMock = vi.mocked(useAvailableAssetProperties);

const TestConsumer: React.FC = () => {
    const context = useAssetPropertiesContext();
    return <div data-testid="context-dump">{JSON.stringify(context)}</div>;
};

describe('AssetPropertiesProvider', () => {
    beforeEach(() => {
        useEntityContextMock.mockReturnValue({
            entityType: EntityType.Dataset,
            urn: 'urn:li:dataset:1',
        } as any);
        useInitialAssetPropertiesMock.mockReturnValue({
            properties: [{ name: 'Owners', type: PropertyType.Owners }],
            loading: false,
        });
        useAvailableAssetPropertiesMock.mockReturnValue({
            availableProperties: [],
            availableStructuredProperties: [],
        });
    });

    it('should initialize with properties and pass them to the context', () => {
        render(
            <AssetPropertiesProvider editable>
                <TestConsumer />
            </AssetPropertiesProvider>,
        );

        const contextValue = JSON.parse(screen.getByTestId('context-dump').textContent || '{}');
        expect(contextValue.properties).toEqual([{ name: 'Owners', type: PropertyType.Owners }]);
        expect(contextValue.propertiesLoading).toBe(false);
        expect(contextValue.editable).toBe(true);
    });

    it('should handle the add action', () => {
        let contextValue: any;
        const TestComponent = () => {
            contextValue = useAssetPropertiesContext();
            return null;
        };

        render(
            <AssetPropertiesProvider editable>
                <TestComponent />
            </AssetPropertiesProvider>,
        );

        act(() => {
            contextValue.add({ name: 'Tags', type: PropertyType.Tags });
        });

        expect(contextValue.properties).toEqual([
            { name: 'Owners', type: PropertyType.Owners },
            { name: 'Tags', type: PropertyType.Tags },
        ]);
    });

    it('should handle the remove action', () => {
        let contextValue: any;
        const TestComponent = () => {
            contextValue = useAssetPropertiesContext();
            return null;
        };

        render(
            <AssetPropertiesProvider editable>
                <TestComponent />
            </AssetPropertiesProvider>,
        );

        act(() => {
            contextValue.remove(0);
        });

        expect(contextValue.properties).toEqual([]);
    });

    it('should handle the replace action', () => {
        let contextValue: any;
        const TestComponent = () => {
            contextValue = useAssetPropertiesContext();
            return null;
        };

        render(
            <AssetPropertiesProvider editable>
                <TestComponent />
            </AssetPropertiesProvider>,
        );

        act(() => {
            contextValue.replace({ name: 'Domain', type: PropertyType.Domain }, 0);
        });

        expect(contextValue.properties).toEqual([{ name: 'Domain', type: PropertyType.Domain }]);
    });
});
