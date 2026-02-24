import { describe, expect, it } from 'vitest';

import { buildViewSelectOptions, resolveViewUrn } from '@app/chat/utils/chatViews';

import { DataHubView, DataHubViewType } from '@types';

describe('buildViewSelectOptions', () => {
    it('should use view description when available', () => {
        const views = [
            {
                urn: 'urn:li:dataHubView:1',
                name: 'My View',
                viewType: DataHubViewType.Personal,
                description: 'Filters for my team datasets',
            },
            {
                urn: 'urn:li:dataHubView:2',
                name: 'Team View',
                viewType: DataHubViewType.Global,
                description: 'All production tables',
            },
        ] as DataHubView[];

        const result = buildViewSelectOptions(views);

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(
            expect.objectContaining({
                value: 'urn:li:dataHubView:1',
                label: 'My View',
                description: 'Filters for my team datasets',
            }),
        );
        expect(result[1]).toEqual(
            expect.objectContaining({
                value: 'urn:li:dataHubView:2',
                label: 'Team View',
                description: 'All production tables',
            }),
        );
        expect(result[0].icon).toBeDefined();
        expect(result[1].icon).toBeDefined();
    });

    it('should have no description when view has none', () => {
        const views = [
            { urn: 'urn:li:dataHubView:1', name: 'My View', viewType: DataHubViewType.Personal },
            { urn: 'urn:li:dataHubView:2', name: 'Team View', viewType: DataHubViewType.Global },
        ] as DataHubView[];

        const result = buildViewSelectOptions(views);

        expect(result).toHaveLength(2);
        expect(result[0].description).toBeUndefined();
        expect(result[1].description).toBeUndefined();
        expect(result[0].icon).toBeDefined();
        expect(result[1].icon).toBeDefined();
    });

    it('should return an empty array when given no views', () => {
        expect(buildViewSelectOptions([])).toEqual([]);
    });
});

describe('resolveViewUrn', () => {
    it('should return the URN when a value is selected', () => {
        expect(resolveViewUrn('urn:li:dataHubView:1')).toBe('urn:li:dataHubView:1');
    });

    it('should return undefined for an empty string', () => {
        expect(resolveViewUrn('')).toBeUndefined();
    });

    it('should return undefined for undefined', () => {
        expect(resolveViewUrn(undefined)).toBeUndefined();
    });
});
