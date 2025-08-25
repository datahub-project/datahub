import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    ASSET_TYPE_DOMAINS,
    ASSET_TYPE_GLOSSARY,
    DEFAULT_ASSET_TYPE,
} from '@app/homeV3/modules/hierarchyViewModule/constants';
import {
    filterAssetUrnsByAssetType,
    getAssetTypeFromAssetUrns,
    isUrnDomainAssetType,
    isUrnGlossaryAssetType,
} from '@app/homeV3/modules/hierarchyViewModule/utils';

import { EntityType } from '@types';

// Mock the extractTypeFromUrn function
const mockExtractTypeFromUrn = vi.hoisted(() => vi.fn());

vi.mock('@app/entity/shared/utils', () => ({
    extractTypeFromUrn: mockExtractTypeFromUrn,
}));

describe('hierarchyViewModule utils', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        // Clear console.warn spy
        vi.spyOn(console, 'warn').mockImplementation(() => {});
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    describe('isUrnDomainAssetType', () => {
        it('should return true for domain URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.Domain);

            const result = isUrnDomainAssetType('urn:li:domain:test');

            expect(result).toBe(true);
            expect(mockExtractTypeFromUrn).toHaveBeenCalledWith('urn:li:domain:test');
        });

        it('should return false for non-domain URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.Dataset);

            const result = isUrnDomainAssetType('urn:li:dataset:test');

            expect(result).toBe(false);
            expect(mockExtractTypeFromUrn).toHaveBeenCalledWith('urn:li:dataset:test');
        });

        it('should return false for glossary URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.GlossaryTerm);

            const result = isUrnDomainAssetType('urn:li:glossaryTerm:test');

            expect(result).toBe(false);
        });
    });

    describe('isUrnGlossaryAssetType', () => {
        it('should return true for glossary term URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.GlossaryTerm);

            const result = isUrnGlossaryAssetType('urn:li:glossaryTerm:test');

            expect(result).toBe(true);
            expect(mockExtractTypeFromUrn).toHaveBeenCalledWith('urn:li:glossaryTerm:test');
        });

        it('should return true for glossary node URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.GlossaryNode);

            const result = isUrnGlossaryAssetType('urn:li:glossaryNode:test');

            expect(result).toBe(true);
            expect(mockExtractTypeFromUrn).toHaveBeenCalledWith('urn:li:glossaryNode:test');
        });

        it('should return false for non-glossary URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.Dataset);

            const result = isUrnGlossaryAssetType('urn:li:dataset:test');

            expect(result).toBe(false);
            expect(mockExtractTypeFromUrn).toHaveBeenCalledWith('urn:li:dataset:test');
        });

        it('should return false for domain URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.Domain);

            const result = isUrnGlossaryAssetType('urn:li:domain:test');

            expect(result).toBe(false);
        });
    });

    describe('getAssetTypeFromAssetUrns', () => {
        it('should return DEFAULT_ASSET_TYPE for undefined urns', () => {
            const result = getAssetTypeFromAssetUrns(undefined);

            expect(result).toBe(DEFAULT_ASSET_TYPE);
        });

        it('should return DEFAULT_ASSET_TYPE for empty array', () => {
            const result = getAssetTypeFromAssetUrns([]);

            expect(result).toBe(DEFAULT_ASSET_TYPE);
        });

        it('should return ASSET_TYPE_DOMAINS for domain URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.Domain);

            const result = getAssetTypeFromAssetUrns(['urn:li:domain:test']);

            expect(result).toBe(ASSET_TYPE_DOMAINS);
            expect(mockExtractTypeFromUrn).toHaveBeenCalledWith('urn:li:domain:test');
        });

        it('should return ASSET_TYPE_GLOSSARY for glossary term URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.GlossaryTerm);

            const result = getAssetTypeFromAssetUrns(['urn:li:glossaryTerm:test']);

            expect(result).toBe(ASSET_TYPE_GLOSSARY);
            expect(mockExtractTypeFromUrn).toHaveBeenCalledWith('urn:li:glossaryTerm:test');
        });

        it('should return ASSET_TYPE_GLOSSARY for glossary node URNs', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.GlossaryNode);

            const result = getAssetTypeFromAssetUrns(['urn:li:glossaryNode:test']);

            expect(result).toBe(ASSET_TYPE_GLOSSARY);
            expect(mockExtractTypeFromUrn).toHaveBeenCalledWith('urn:li:glossaryNode:test');
        });

        it('should return DEFAULT_ASSET_TYPE and warn for unsupported URN types', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.Dataset);
            const consoleSpy = vi.spyOn(console, 'warn');

            const result = getAssetTypeFromAssetUrns(['urn:li:dataset:test']);

            expect(result).toBe(DEFAULT_ASSET_TYPE);
            expect(consoleSpy).toHaveBeenCalledWith('Unsupportable urn:', 'urn:li:dataset:test');
        });

        it('should use the first URN in the array', () => {
            mockExtractTypeFromUrn.mockReturnValue(EntityType.Domain);

            const result = getAssetTypeFromAssetUrns(['urn:li:domain:test1', 'urn:li:glossaryTerm:test2']);

            expect(result).toBe(ASSET_TYPE_DOMAINS);
            expect(mockExtractTypeFromUrn).toHaveBeenCalledWith('urn:li:domain:test1');
            expect(mockExtractTypeFromUrn).toHaveBeenCalledTimes(1);
        });
    });

    describe('filterAssetUrnsByAssetType', () => {
        it('should return empty array for undefined urns', () => {
            const result = filterAssetUrnsByAssetType(undefined, ASSET_TYPE_DOMAINS);

            expect(result).toEqual([]);
        });

        it('should return empty array for empty urns array', () => {
            const result = filterAssetUrnsByAssetType([], ASSET_TYPE_DOMAINS);

            expect(result).toEqual([]);
        });

        it('should filter domain URNs when asset type is domains', () => {
            mockExtractTypeFromUrn.mockImplementation((urn) => {
                if (urn.includes('domain')) return EntityType.Domain;
                if (urn.includes('glossary')) return EntityType.GlossaryTerm;
                return EntityType.Dataset;
            });

            const urns = [
                'urn:li:domain:test1',
                'urn:li:glossaryTerm:test2',
                'urn:li:domain:test3',
                'urn:li:dataset:test4',
            ];

            const result = filterAssetUrnsByAssetType(urns, ASSET_TYPE_DOMAINS);

            expect(result).toEqual(['urn:li:domain:test1', 'urn:li:domain:test3']);
        });

        it('should filter glossary URNs when asset type is glossary', () => {
            mockExtractTypeFromUrn.mockImplementation((urn) => {
                if (urn.includes('domain')) return EntityType.Domain;
                if (urn.includes('glossaryTerm')) return EntityType.GlossaryTerm;
                if (urn.includes('glossaryNode')) return EntityType.GlossaryNode;
                return EntityType.Dataset;
            });

            const urns = [
                'urn:li:domain:test1',
                'urn:li:glossaryTerm:test2',
                'urn:li:glossaryNode:test3',
                'urn:li:dataset:test4',
            ];

            const result = filterAssetUrnsByAssetType(urns, ASSET_TYPE_GLOSSARY);

            expect(result).toEqual(['urn:li:glossaryTerm:test2', 'urn:li:glossaryNode:test3']);
        });

        it('should return empty array and warn for unsupported asset types', () => {
            const consoleSpy = vi.spyOn(console, 'warn');
            const urns = ['urn:li:domain:test1'];

            const result = filterAssetUrnsByAssetType(urns, 'unsupported' as any);

            expect(result).toEqual([]);
            expect(consoleSpy).toHaveBeenCalledWith('Unsupportable assetType:', 'unsupported');
        });

        it('should handle mixed URN types correctly for domain filtering', () => {
            mockExtractTypeFromUrn.mockImplementation((urn) => {
                if (urn === 'urn:li:domain:1') return EntityType.Domain;
                if (urn === 'urn:li:domain:2') return EntityType.Domain;
                return EntityType.GlossaryTerm;
            });

            const urns = ['urn:li:domain:1', 'urn:li:glossaryTerm:1', 'urn:li:domain:2', 'urn:li:glossaryTerm:2'];

            const result = filterAssetUrnsByAssetType(urns, ASSET_TYPE_DOMAINS);

            expect(result).toEqual(['urn:li:domain:1', 'urn:li:domain:2']);
        });
    });
});
