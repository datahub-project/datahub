import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { Avatar } from '@components/components/Avatar';
import { AvatarType } from '@components/components/AvatarStack/types';

import CustomThemeProvider from '@src/CustomThemeProvider';

describe('Avatar', () => {
    it('renders the given icon for a role without an image', () => {
        render(
            <CustomThemeProvider>
                <Avatar name="Admin" type={AvatarType.role} icon={<span data-testid="role-icon" />} />
            </CustomThemeProvider>,
        );

        expect(screen.getByTestId('role-icon')).toBeInTheDocument();
    });

    it('falls back to a default icon for a role without an image or icon', () => {
        const { container } = render(
            <CustomThemeProvider>
                <Avatar name="Admin" type={AvatarType.role} />
            </CustomThemeProvider>,
        );

        expect(container.querySelector('svg')).toBeInTheDocument();
    });
});
