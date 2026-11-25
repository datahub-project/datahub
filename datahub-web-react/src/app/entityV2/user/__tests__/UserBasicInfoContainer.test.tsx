import { MockedProvider } from '@apollo/client/testing';
import { getDefaultNormalizer, render, screen } from '@testing-library/react';
import React from 'react';

import { UserBasicInfoContainer } from '@app/entityV2/user/UserBasicInfoContainer';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const BASE_PROPS = {
    name: 'Jane Doe',
    dataHubRoleName: '',
    email: 'jane@example.com',
    role: 'Engineer',
    slack: undefined,
    phone: undefined,
};

describe('UserBasicInfoContainer', () => {
    it('shows the team and links the manager to their profile', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <UserBasicInfoContainer
                        {...BASE_PROPS}
                        team="Data Platform, Analytics,"
                        manager={{ urn: 'urn:li:corpuser:jdoe', name: 'John Doe' }}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        // Exact whitespace: a department that already contains ", " must not gain a second space.
        expect(
            screen.getByText('Data Platform, Analytics', {
                normalizer: getDefaultNormalizer({ trim: false, collapseWhitespace: false }),
            }),
        ).toBeInTheDocument();
        expect(screen.getByRole('link', { name: 'John Doe' })).toHaveAttribute('href', '/user/urn:li:corpuser:jdoe');
    });

    it('hides the team and manager rows when they are empty', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <UserBasicInfoContainer {...BASE_PROPS} team=" , " manager={undefined} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getByText('jane@example.com')).toBeInTheDocument();
        expect(screen.queryByText(/,/)).not.toBeInTheDocument();
        expect(screen.queryByRole('link')).not.toBeInTheDocument();
    });
});
