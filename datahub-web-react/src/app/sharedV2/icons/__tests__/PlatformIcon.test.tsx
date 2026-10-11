import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { DataPlatform, EntityType, PlatformType } from '@types';

vi.mock('@app/sharedV2/logical/LogicalPlatformDefaultIcon', () => ({
    default: () => <div data-testid="logical-platform-default-icon" />,
}));

function platform(logical: boolean, logoUrl?: string): DataPlatform {
    return {
        urn: 'urn:li:dataPlatform:myLogicalSystem',
        name: 'myLogicalSystem',
        type: EntityType.DataPlatform,
        properties: { logical, logoUrl, type: PlatformType.Others },
    } as DataPlatform;
}

function renderIcon(p: DataPlatform) {
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <PlatformIcon platform={p} />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('PlatformIcon', () => {
    it('shows the logical default icon for a logical platform without a custom logo', () => {
        renderIcon(platform(true));

        expect(screen.getByTestId('logical-platform-default-icon')).toBeInTheDocument();
    });

    it('prefers a custom logo over the logical default icon', () => {
        renderIcon(platform(true, 'https://example.com/logo.png'));

        expect(screen.queryByTestId('logical-platform-default-icon')).not.toBeInTheDocument();
        expect(screen.getByRole('img')).toHaveAttribute('src', 'https://example.com/logo.png');
    });

    it('does not show the logical default icon for a non-logical platform', () => {
        renderIcon(platform(false));

        expect(screen.queryByTestId('logical-platform-default-icon')).not.toBeInTheDocument();
    });
});
