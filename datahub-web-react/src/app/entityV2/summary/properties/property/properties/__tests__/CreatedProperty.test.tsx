import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import EntityContext from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import CreatedProperty from '@app/entityV2/summary/properties/property/properties/CreatedProperty';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { Document, DocumentSourceType, EntityType, SummaryElementType } from '@types';

vi.mock('@app/entityV2/summary/properties/property/properties/BaseProperty', () => ({
    default: ({ values }: { values: number[] }) => (
        <div data-testid="base-property">{values.length > 0 ? 'has-value' : 'empty'}</div>
    ),
}));

const PROP = { type: SummaryElementType.Created, name: 'Created' };

const makeContext = (document: Partial<Document>) => ({
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

const renderProp = (document: Partial<Document>) =>
    render(
        <CustomThemeProvider>
            <EntityContext.Provider value={makeContext(document)}>
                <CreatedProperty property={PROP} position={0} />
            </EntityContext.Provider>
        </CustomThemeProvider>,
    );

describe('CreatedProperty — external document heuristic', () => {
    it('shows created date for native documents unconditionally', () => {
        renderProp({
            type: EntityType.Document,
            info: {
                title: 'Doc',
                contents: { text: '' },
                created: { time: 1000 },
                lastModified: { time: 500 },
                source: { sourceType: DocumentSourceType.Native },
            },
        });
        expect(screen.getByTestId('base-property').textContent).toBe('has-value');
    });

    it('shows created date for external docs when created < lastModified (real creation date)', () => {
        renderProp({
            type: EntityType.Document,
            info: {
                title: 'Doc',
                contents: { text: '' },
                created: { time: 1000 },
                lastModified: { time: 2000 },
                source: { sourceType: DocumentSourceType.External },
            },
        });
        expect(screen.getByTestId('base-property').textContent).toBe('has-value');
    });

    it('hides created date for external docs when created >= lastModified (connector used ingestion time)', () => {
        renderProp({
            type: EntityType.Document,
            info: {
                title: 'Doc',
                contents: { text: '' },
                created: { time: 2000 },
                lastModified: { time: 1000 },
                source: { sourceType: DocumentSourceType.External },
            },
        });
        expect(screen.getByTestId('base-property').textContent).toBe('empty');
    });

    it('hides created date for external docs when created equals lastModified', () => {
        renderProp({
            type: EntityType.Document,
            info: {
                title: 'Doc',
                contents: { text: '' },
                created: { time: 1000 },
                lastModified: { time: 1000 },
                source: { sourceType: DocumentSourceType.External },
            },
        });
        expect(screen.getByTestId('base-property').textContent).toBe('empty');
    });

    it('hides created date for external docs when lastModified is missing', () => {
        renderProp({
            type: EntityType.Document,
            info: {
                title: 'Doc',
                contents: { text: '' },
                created: { time: 1000 },
                source: { sourceType: DocumentSourceType.External },
            } as any,
        });
        expect(screen.getByTestId('base-property').textContent).toBe('empty');
    });
});
