import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useDomainsContext } from '@app/domainV2/DomainsContext';
import { useHandleDeprecateDomain } from '@app/entityV2/shared/EntityDropdown/useHandleDeprecateDomain';

import { EntityType } from '@types';

vi.mock('@app/domainV2/DomainsContext', () => ({
    useDomainsContext: vi.fn(),
}));

const DOMAIN_URN = 'urn:li:domain:test-domain';

describe('useHandleDeprecateDomain', () => {
    let mockSetUpdatedDomain: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        mockSetUpdatedDomain = vi.fn();
        vi.mocked(useDomainsContext).mockReturnValue({ setUpdatedDomain: mockSetUpdatedDomain } as any);
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('sets deprecation with note, decommissionTime, and replacement when deprecated=true', () => {
        const mockReplacement = { urn: 'urn:li:domain:accounts', type: EntityType.Domain } as any;
        const { result } = renderHook(() => useHandleDeprecateDomain(DOMAIN_URN));

        result.current.handleDeprecateDomainComplete(true, {
            note: 'Moving to new domain',
            decommissionTime: 1750000000000,
            replacement: mockReplacement,
        });

        expect(mockSetUpdatedDomain).toHaveBeenCalledWith(
            expect.objectContaining({
                urn: DOMAIN_URN,
                type: EntityType.Domain,
                deprecation: expect.objectContaining({
                    deprecated: true,
                    note: 'Moving to new domain',
                    decommissionTime: 1750000000000,
                    replacement: mockReplacement,
                }),
            }),
        );
    });

    it('sets deprecation with null replacement when no replacement is provided', () => {
        const { result } = renderHook(() => useHandleDeprecateDomain(DOMAIN_URN));

        result.current.handleDeprecateDomainComplete(true, {
            note: 'Deprecated',
            decommissionTime: null,
        });

        expect(mockSetUpdatedDomain).toHaveBeenCalledWith(
            expect.objectContaining({
                deprecation: expect.objectContaining({
                    deprecated: true,
                    replacement: null,
                }),
            }),
        );
    });

    it('sets deprecation to null when deprecated=false', () => {
        const { result } = renderHook(() => useHandleDeprecateDomain(DOMAIN_URN));

        result.current.handleDeprecateDomainComplete(false);

        expect(mockSetUpdatedDomain).toHaveBeenCalledWith(
            expect.objectContaining({
                urn: DOMAIN_URN,
                deprecation: null,
            }),
        );
    });

    it('sets all optional fields to null when deprecated=true with no formData', () => {
        const { result } = renderHook(() => useHandleDeprecateDomain(DOMAIN_URN));

        result.current.handleDeprecateDomainComplete(true);

        expect(mockSetUpdatedDomain).toHaveBeenCalledWith(
            expect.objectContaining({
                deprecation: expect.objectContaining({
                    deprecated: true,
                    note: null,
                    decommissionTime: null,
                    replacement: null,
                }),
            }),
        );
    });
});
