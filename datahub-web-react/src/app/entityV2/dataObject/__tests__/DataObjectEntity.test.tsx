import { describe, expect, it } from 'vitest';

import { DataObjectEntity } from '@app/entityV2/dataObject/DataObjectEntity';

import { EntityType } from '@types';

describe('DataObjectEntity', () => {
    const e = new DataObjectEntity();

    it('has the right type and graph name', () => {
        expect(e.type).toBe(EntityType.DataObject);
        expect(e.getGraphName()).toBe('dataObject');
    });

    it('displayName prefers properties.name', () => {
        expect(e.displayName({ urn: 'u', properties: { name: 'clip.mp4' } } as any)).toBe('clip.mp4');
    });

    it('displayName falls back to data.name then urn', () => {
        expect(e.displayName({ urn: 'u', name: 'fallback.mp4' } as any)).toBe('fallback.mp4');
        expect(e.displayName({ urn: 'urn:li:dataObject:test' } as any)).toBe('urn:li:dataObject:test');
    });

    it('getPathName returns dataObject', () => {
        expect(e.getPathName()).toBe('dataObject');
    });

    it('isLineageEnabled returns false', () => {
        expect(e.isLineageEnabled()).toBe(false);
    });

    it('isSearchEnabled returns true', () => {
        expect(e.isSearchEnabled()).toBe(true);
    });
});
