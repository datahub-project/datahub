import { render, act } from '@testing-library/react';
import React, { useContext } from 'react';
import { describe, it, expect, vi } from 'vitest';
import { useEntityContext } from '@app/entity/shared/EntityContext';
import AssetPropertiesContext from '@app/entityV2/summary/properties/context/AssetPropertiesContext';
import useInitialAssetProperties from '@app/entityV2/summary/properties/hooks/useInitialAssetProperties';
import { PropertyType } from '@app/entityV2/summary/properties/types';
import { EntityType } from '@types';
import AssetPropertiesProvider from '@app/entityV2/summary/properties/context/AssetPropertiesProvider';

vi.mock('@app/entity/shared/EntityContext');
vi.mock('@app/entityV2/summary/properties/hooks/useInitialAssetProperties');

const initialProperties = [
    { name: 'prop1', type: PropertyType.Domain },
    { name: 'prop2', type: PropertyType.Owners },
];

const TestConsumer = () => {
    const context = useContext(AssetPropertiesContext);
    return (
        <div>
            <div data-testid="loading">{String(context.propertiesLoading)}</div>
            <div data-testid="properties">{JSON.stringify(context.properties)}</div>
            <button type="button" onClick={() => context.add({ name: 'newProp', type: PropertyType.Tags })}>
                add
            </button>
            <button type="button" onClick={() => context.remove(0)}>
                remove
            </button>
            <button type="button" onClick={() => context.replace({ name: 'replaced', type: PropertyType.Terms }, 1)}>
                replace
            </button>
        </div>
    );
};

describe('AssetPropertiesProvider', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        (useEntityContext as any).mockReturnValue({
            urn: 'test-urn',
            entityType: EntityType.Dataset,
        });
        (useInitialAssetProperties as any).mockReturnValue({
            properties: initialProperties,
            loading: false,
        });
    });

    it('should provide initial properties and loading state', () => {
        const { getByTestId } = render(
            <AssetPropertiesProvider editable>
                <TestConsumer />
            </AssetPropertiesProvider>,
        );
        expect(getByTestId('loading').textContent).toBe('false');
        expect(JSON.parse(getByTestId('properties').textContent || '')).toEqual(initialProperties);
    });

    it('should handle the add function', () => {
        const { getByTestId, getByText } = render(
            <AssetPropertiesProvider editable>
                <TestConsumer />
            </AssetPropertiesProvider>,
        );
        act(() => {
            getByText('add').click();
        });
        const properties = JSON.parse(getByTestId('properties').textContent || '');
        expect(properties).toHaveLength(3);
        expect(properties[2].name).toBe('newProp');
    });

    it('should handle the remove function', () => {
        const { getByTestId, getByText } = render(
            <AssetPropertiesProvider editable>
                <TestConsumer />
            </AssetPropertiesProvider>,
        );
        act(() => {
            getByText('remove').click();
        });
        const properties = JSON.parse(getByTestId('properties').textContent || '');
        expect(properties).toHaveLength(1);
        expect(properties[0].name).toBe('prop2');
    });

    it('should handle the replace function', () => {
        const { getByTestId, getByText } = render(
            <AssetPropertiesProvider editable>
                <TestConsumer />
            </AssetPropertiesProvider>,
        );
        act(() => {
            getByText('replace').click();
        });
        const properties = JSON.parse(getByTestId('properties').textContent || '');
        expect(properties).toHaveLength(2);
        expect(properties[1].name).toBe('replaced');
    });
});
