import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import EntityContext from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import LastIngestedProperty from '@app/entityV2/summary/properties/property/properties/LastIngestedProperty';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { Document, DocumentSourceType, EntityType, SummaryElementType } from '@types';

vi.mock('@app/entityV2/summary/properties/property/properties/BaseProperty', () => ({
    default: ({ values }: { values: number[] }) => (
        <div data-testid="base-property">{values.length > 0 ? 'has-value' : 'empty'}</div>
    ),
}));

const PROP = { type: SummaryElementType.LastIngested, name: 'Last Synced' };

const makeContext = (document: Partial<Document> & { lastIngested?: number | null }) => ({
    urn: 'urn:li:document:test',
    entityType: EntityType.Document,
    entityData: document as unknown as GenericEntityProperties,
    loading: false,
    baseEntity: document as unknown as GenericEntityProperties,
    dataNotCombinedWithSiblings: undefined,
    routeToTab: () => {},
    refetch: async () => ({}),
    lineage: undefined,
});

const renderProp = (document: Partial<Document> & { lastIngested?: number | null }) =>
    render(
        <CustomThemeProvider>
            <EntityContext.Provider value={makeContext(document)}>
                <LastIngestedProperty property={PROP} position={0} />
            </EntityContext.Provider>
        </CustomThemeProvider>,
    );

describe('LastIngestedProperty — external-only gate', () => {
    it('shows lastIngested for external documents', () => {
        renderProp({
            type: EntityType.Document,
            lastIngested: 1716000000000,
            info: {
                title: 'Doc',
                contents: { text: '' },
                source: { sourceType: DocumentSourceType.External },
            } as any,
        });
        expect(screen.getByTestId('base-property').textContent).toBe('has-value');
    });

    it('hides lastIngested for native documents even when value is present', () => {
        renderProp({
            type: EntityType.Document,
            lastIngested: 1716000000000,
            info: {
                title: 'Doc',
                contents: { text: '' },
                source: { sourceType: DocumentSourceType.Native },
            } as any,
        });
        expect(screen.getByTestId('base-property').textContent).toBe('empty');
    });

    it('hides lastIngested when source type is not set', () => {
        renderProp({
            type: EntityType.Document,
            lastIngested: 1716000000000,
            info: { title: 'Doc', contents: { text: '' } } as any,
        });
        expect(screen.getByTestId('base-property').textContent).toBe('empty');
    });

    it('hides lastIngested for external documents when value is null', () => {
        renderProp({
            type: EntityType.Document,
            lastIngested: null,
            info: {
                title: 'Doc',
                contents: { text: '' },
                source: { sourceType: DocumentSourceType.External },
            } as any,
        });
        expect(screen.getByTestId('base-property').textContent).toBe('empty');
    });
});
