import { render } from '@testing-library/react';
import React from 'react';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useShowActorPoliciesTab } from '@app/permissions/actorPolicies/useShowActorPoliciesTab';

const mockUserContext = vi.fn();
const mockAppConfig = vi.fn();

vi.mock('@app/context/useUserContext', () => ({ useUserContext: () => mockUserContext() }));
vi.mock('@app/useAppConfig', () => ({ useAppConfig: () => mockAppConfig() }));

const ME = 'urn:li:corpuser:me';

const setup = ({ admin = false, flag = true }: { admin?: boolean; flag?: boolean }) => {
    (mockUserContext as Mock).mockReturnValue({ urn: ME, platformPrivileges: { managePolicies: admin } });
    (mockAppConfig as Mock).mockReturnValue({ config: { featureFlags: { actorPoliciesSelfServiceEnabled: flag } } });
};

const show = (params: { profileUrn: string; isGroup: boolean; isMember: boolean }) => {
    let result = false;
    const Probe = () => {
        result = useShowActorPoliciesTab(params);
        return null;
    };
    render(<Probe />);
    return result;
};

describe('useShowActorPoliciesTab', () => {
    beforeEach(() => vi.clearAllMocks());

    it('shows for an admin on any profile, even with the flag off', () => {
        setup({ admin: true, flag: false });
        expect(show({ profileUrn: 'urn:li:corpuser:other', isGroup: false, isMember: false })).toBe(true);
        expect(show({ profileUrn: 'urn:li:corpGroup:g', isGroup: true, isMember: false })).toBe(true);
    });

    it('shows on my own profile only when the flag is on', () => {
        setup({ flag: true });
        expect(show({ profileUrn: ME, isGroup: false, isMember: false })).toBe(true);
        setup({ flag: false });
        expect(show({ profileUrn: ME, isGroup: false, isMember: false })).toBe(false);
    });

    it('hides on another user profile for a non-admin', () => {
        setup({ flag: true });
        expect(show({ profileUrn: 'urn:li:corpuser:other', isGroup: false, isMember: false })).toBe(false);
    });

    it('shows on a group I belong to when the flag is on, hides otherwise', () => {
        setup({ flag: true });
        expect(show({ profileUrn: 'urn:li:corpGroup:g', isGroup: true, isMember: true })).toBe(true);
        expect(show({ profileUrn: 'urn:li:corpGroup:g', isGroup: true, isMember: false })).toBe(false);
        setup({ flag: false });
        expect(show({ profileUrn: 'urn:li:corpGroup:g', isGroup: true, isMember: true })).toBe(false);
    });
});
