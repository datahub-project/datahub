import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import { GlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import OrganizationInfo from '@app/settingsV2/OrganizationInfo';

const mockContext = {
    globalSettings: {
        visualSettings: { customOrgName: 'Test Org' },
        integrationSettings: {},
        notificationSettings: { settings: [] },
    },
    helpLinkState: {
        isEnabled: false,
        setIsEnabled: vi.fn(),
        label: 'Contact Admin',
        setLabel: vi.fn(),
        link: '',
        setLink: vi.fn(),
        resetHelpLinkState: vi.fn(),
    },
    refetch: vi.fn(),
    loading: false,
};

const TestWrapper = ({ children }: { children: React.ReactNode }) => (
    <MockedProvider>
        <GlobalSettingsContext.Provider value={mockContext}>{children}</GlobalSettingsContext.Provider>
    </MockedProvider>
);

describe('OrganizationInfo', () => {
    it('shows character counter when typing', () => {
        render(<OrganizationInfo />, { wrapper: TestWrapper });

        const input = screen.getByPlaceholderText('Organization Name');
        fireEvent.change(input, { target: { value: 'Hello' } });

        expect(screen.getByText('5/25 characters')).toBeInTheDocument();
    });

    it('hides character counter when empty', () => {
        render(<OrganizationInfo />, { wrapper: TestWrapper });

        const input = screen.getByPlaceholderText('Organization Name');
        fireEvent.change(input, { target: { value: '' } });

        expect(screen.queryByText('/25 characters')).not.toBeInTheDocument();
    });
});
