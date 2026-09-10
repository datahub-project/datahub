import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { CompactEntityNameComponent } from '@app/recommendations/renderer/component/CompactEntityNameComponent';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { Entity, EntityType } from '@types';

const mockEntityRegistry = {
    getGenericEntityProperties: () => ({ platform: null }),
    getDisplayName: () => 'my_dataset',
    getIcon: () => null,
    getEntityUrl: () => '/dataset/urn:li:dataset:test',
    renderPreview: () => null,
};

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => mockEntityRegistry,
    useEntityRegistryV2: () => mockEntityRegistry,
}));

const renderWithProviders = (ui: React.ReactElement) =>
    render(
        <CustomThemeProvider>
            <MemoryRouter>{ui}</MemoryRouter>
        </CustomThemeProvider>,
    );

describe('CompactEntityNameComponent', () => {
    it('decodes percent-encoded characters in a schema field name instead of showing them raw', () => {
        const schemaFieldEntity = {
            urn: 'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_table,PROD),Revenue %28Net%29)',
            type: EntityType.SchemaField,
            fieldPath: 'Revenue %28Net%29',
            parent: {
                urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_table,PROD)',
                type: EntityType.Dataset,
            },
        } as unknown as Entity;

        renderWithProviders(<CompactEntityNameComponent entity={schemaFieldEntity} />);

        expect(screen.getByText('Revenue (Net)')).toBeInTheDocument();
        expect(screen.queryByText('Revenue %28Net%29')).not.toBeInTheDocument();
    });
});
