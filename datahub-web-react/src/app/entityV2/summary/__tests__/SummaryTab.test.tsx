import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import SummaryTab from '@app/entityV2/summary/SummaryTab';

vi.mock('@app/entityV2/shared/EntityContext', () => ({ useEntityData: () => ({ urn: 'urn:li:dataset:x' }) }));
vi.mock('@app/entityV2/summary/properties/PropertiesHeader', () => ({ default: () => <div data-testid="properties" /> }));
vi.mock('@app/entityV2/summary/documentation/AboutSection', () => ({ default: () => <div data-testid="about" /> }));
vi.mock('@app/homeV3/template/Template', () => ({ default: () => <div data-testid="template" /> }));
vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    PageTemplateProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));
vi.mock('@app/mfeframework/slots/MFEEntityRegion', () => ({
    default: ({ slot }: { slot: string }) => <div data-testid={`region-${slot}`} />,
}));

describe('SummaryTab', () => {
    it('frames its content with the top and bottom micro frontend regions', () => {
        render(
            <ThemeProvider theme={{ colors: { border: '#eee' } } as any}>
                <SummaryTab />
            </ThemeProvider>,
        );
        const order = screen.getAllByTestId(/^(region-|properties|about|template)/).map((el) => el.dataset.testid);
        expect(order).toEqual([
            'region-entity.summary.top',
            'properties',
            'about',
            'template',
            'region-entity.summary.bottom',
        ]);
    });
});
