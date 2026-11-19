import { render } from '@testing-library/react';
import React from 'react';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useShowActorPoliciesTab } from '@app/permissions/actorPolicies/useShowActorPoliciesTab';

const mockUserContext = vi.fn();
const mockAppConfig = vi.fn();
const mockGroupUrns = vi.fn();

vi.mock('@app/context/useUserContext', () => ({ useUserContext: () => mockUserContext() }));
vi.mock('@app/useAppConfig', () => ({ useAppConfig: () => mockAppConfig() }));
vi.mock('@app/entityV2/user/useGetUserGroupUrns', () => ({ default: () => mockGroupUrns() }));

const ME = 'urn:li:corpuser:me';
const GROUP = 'urn:li:corpGroup:g';

const setup = ({
    admin = false,
    flag = true,
    myGroups = [],
    myGroupsTotal = myGroups.length,
}: {
    admin?: boolean;
    flag?: boolean;
    myGroups?: string[];
    myGroupsTotal?: number;
}) => {
    (mockUserContext as Mock).mockReturnValue({ urn: ME, platformPrivileges: { managePolicies: admin } });
    (mockAppConfig as Mock).mockReturnValue({ config: { featureFlags: { actorPoliciesSelfServiceEnabled: flag } } });
    (mockGroupUrns as Mock).mockReturnValue({
        groupUrns: myGroups,
        data: { corpUser: { relationships: { total: myGroupsTotal } } },
    });
};

const show = (params: { profileUrn: string; isGroup: boolean }) => {
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
        expect(show({ profileUrn: 'urn:li:corpuser:other', isGroup: false })).toBe(true);
        expect(show({ profileUrn: GROUP, isGroup: true })).toBe(true);
    });

    it('shows on my own profile only when the flag is on', () => {
        setup({ flag: true });
        expect(show({ profileUrn: ME, isGroup: false })).toBe(true);
        setup({ flag: false });
        expect(show({ profileUrn: ME, isGroup: false })).toBe(false);
    });

    it('hides on another user profile for a non-admin', () => {
        setup({ flag: true });
        expect(show({ profileUrn: 'urn:li:corpuser:other', isGroup: false })).toBe(false);
    });

    it('shows on a group I belong to when the flag is on, hides otherwise', () => {
        setup({ flag: true, myGroups: [GROUP] });
        expect(show({ profileUrn: GROUP, isGroup: true })).toBe(true);
        setup({ flag: true, myGroups: ['urn:li:corpGroup:other'] });
        expect(show({ profileUrn: GROUP, isGroup: true })).toBe(false);
        setup({ flag: false, myGroups: [GROUP] });
        expect(show({ profileUrn: GROUP, isGroup: true })).toBe(false);
    });

    it('shows on a group when I have more groups than were fetched', () => {
        setup({ flag: true, myGroups: ['urn:li:corpGroup:other'], myGroupsTotal: 150 });
        expect(show({ profileUrn: GROUP, isGroup: true })).toBe(true);
    });
});
