import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { OnboardingCards } from '@app/homeV2/content/tabs/discovery/sections/onboarding/OnboardingCards';
import { render, screen } from '@utils/test-utils/customRender';

const mockUserContext = {
    loaded: true,
    user: { urn: 'urn:li:corpuser:tester' },
    platformPrivileges: {
        managePolicies: false,
        manageUserCredentials: false,
    },
};

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => mockUserContext,
}));

vi.mock('@src/app/homeV2/content/tabs/discovery/sections/platform/useGetPlatforms', () => ({
    useGetPlatforms: () => ({ platforms: [], loading: false }),
}));

describe('OnboardingCards', () => {
    beforeEach(() => {
        mockUserContext.platformPrivileges.managePolicies = false;
        mockUserContext.platformPrivileges.manageUserCredentials = false;
    });

    it('hides the invite card when the caller can manage policies but not credentials', () => {
        mockUserContext.platformPrivileges.managePolicies = true;
        render(<OnboardingCards />);

        expect(screen.queryByText('Invite Users')).not.toBeInTheDocument();
    });

    it('shows the invite card when the caller can manage user credentials', () => {
        mockUserContext.platformPrivileges.manageUserCredentials = true;
        render(<OnboardingCards />);

        expect(screen.getByText('Invite Users')).toBeInTheDocument();
    });
});
