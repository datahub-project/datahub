import { describe, expect, it } from 'vitest';

import { buildViewSelectOptions, resolveViewUrn } from '@app/chat/utils/chatViews';

import { DataHubView, DataHubViewType } from '@types';

describe('buildViewSelectOptions', () => {
    it('should map views to select options with type as description', () => {
        const views = [
            { urn: 'urn:li:dataHubView:1', name: 'My View', viewType: DataHubViewType.Personal },
            { urn: 'urn:li:dataHubView:2', name: 'Team View', viewType: DataHubViewType.Global },
        ] as DataHubView[];

        const result = buildViewSelectOptions(views);

        expect(result).toEqual([
            { value: 'urn:li:dataHubView:1', label: 'My View', description: 'Personal' },
            { value: 'urn:li:dataHubView:2', label: 'Team View', description: 'Global' },
        ]);
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
