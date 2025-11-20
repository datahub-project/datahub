import { render } from '@testing-library/react';
import { act, renderHook } from '@testing-library/react-hooks';
import { createMemoryHistory } from 'history';
import React from 'react';
import { Router } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import useDomainModuleUtils from '@app/homeV3/modules/domains/useDomainModuleUtils';
import { PageRoutes } from '@conf/Global';

import { Domain, Entity, EntityType } from '@types';

// Mock Text component
vi.mock('@components', () => ({
    Text: ({ children }: { children: React.ReactNode }) => <span>{children}</span>,
}));

describe('useDomainModuleUtils', () => {
    const domains = [
        {
            entity: {
                urn: 'urn:li:domain:1',
                dataProducts: { total: 2 },
                type: EntityType.Domain,
                id: '1',
            } as Domain,
            assetCount: 5,
        },
        {
            entity: {
                urn: 'urn:li:domain:2',
                dataProducts: { total: 0 },
                type: EntityType.Domain,
                id: '2',
            } as Domain,
            assetCount: 0,
        },
        {
            entity: {
                urn: 'urn:li:domain:3',
                dataProducts: { total: 0 },
                type: EntityType.Domain,
                id: '3',
            } as Domain,
            assetCount: 7,
        },
    ];

    it('should call history.push with correct route whe navigating to domains', () => {
        const history = createMemoryHistory();
        const wrapperWithHistory = ({ children }: { children: React.ReactNode }) => (
            <Router history={history}>{children}</Router>
        );

        const { result } = renderHook(() => useDomainModuleUtils({ domains }), { wrapper: wrapperWithHistory });

        act(() => {
            result.current.navigateToDomains();
        });

        expect(history.location.pathname).toBe(PageRoutes.DOMAINS);
    });

    it('should return correct JSX for domain with assets and data products', () => {
        const { result } = renderHook(() => useDomainModuleUtils({ domains }));

        const entity: Entity = { urn: 'urn:li:domain:1' } as Entity;
        const jsx = result.current.renderDomainCounts(entity);

        const { container } = render(jsx);

        expect(container.textContent).toContain('5 assets');
        expect(container.textContent).toContain(', ');
        expect(container.textContent).toContain('2 data products');
    });

    it('should return correct JSX for domain with assets but no data products', () => {
        const { result } = renderHook(() => useDomainModuleUtils({ domains }));

        const entity: Entity = { urn: 'urn:li:domain:3' } as Entity;
        const jsx = result.current.renderDomainCounts(entity);

        const { container } = render(jsx);
        expect(container.textContent).not.toContain(', ');
        expect(container.textContent).toContain('7 assets');

        expect(container.textContent).not.toContain('data product');
    });

    it('should return correct JSX for domain with no assets or data products', () => {
        const { result } = renderHook(() => useDomainModuleUtils({ domains }));

        const entity: Entity = { urn: 'urn:li:domain:2' } as Entity;
        const jsx = result.current.renderDomainCounts(entity);

        const { container } = render(jsx);

        expect(container.textContent).toBe('');
    });

    it('should return empty fragment for unknown domain', () => {
        const { result } = renderHook(() => useDomainModuleUtils({ domains }));

        const entity: Entity = { urn: 'urn:li:domain:unknown' } as Entity;
        const jsx = result.current.renderDomainCounts(entity);

        const { container } = render(jsx);

        expect(container.textContent).toBe('');
    });
});
