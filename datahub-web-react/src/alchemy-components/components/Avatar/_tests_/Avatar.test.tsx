import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { Avatar } from '@components/components/Avatar';
import { AvatarType } from '@components/components/AvatarStack/types';

import CustomThemeProvider from '@src/CustomThemeProvider';

describe('Avatar', () => {
    it('renders the given icon for a role without an image', () => {
        const { container } = render(
            <CustomThemeProvider>
                <Avatar name="Admin" type={AvatarType.role} icon={<span data-testid="role-icon" />} />
            </CustomThemeProvider>,
        );

        expect(screen.getByTestId('role-icon')).toBeInTheDocument();
        expect(container.querySelector('svg')).not.toBeInTheDocument();
    });

    it('falls back to a default icon for a role without an image or icon', () => {
        const { container } = render(
            <CustomThemeProvider>
                <Avatar name="Admin" type={AvatarType.role} />
            </CustomThemeProvider>,
        );

        expect(container.querySelector('svg')).toBeInTheDocument();
    });

    it('renders the given icon instead of initials for a user without an image', () => {
        render(
            <CustomThemeProvider>
                <Avatar name="Jane Doe" type={AvatarType.user} icon={<span data-testid="user-icon" />} />
            </CustomThemeProvider>,
        );

        expect(screen.getByTestId('user-icon')).toBeInTheDocument();
        expect(screen.queryByText('JD')).not.toBeInTheDocument();
    });

    it('renders the given icon instead of the default icon for a group without an image', () => {
        const { container } = render(
            <CustomThemeProvider>
                <Avatar name="Data Team" type={AvatarType.group} icon={<span data-testid="group-icon" />} />
            </CustomThemeProvider>,
        );

        expect(screen.getByTestId('group-icon')).toBeInTheDocument();
        expect(container.querySelector('svg')).not.toBeInTheDocument();
    });

    it('replaces the built-in circle with a custom avatar and keeps the pill name', () => {
        render(
            <CustomThemeProvider>
                <Avatar
                    name="Jane Doe"
                    showInPill
                    imageUrl="https://example.com/jane.png"
                    avatar={<span data-testid="custom-avatar" />}
                />
            </CustomThemeProvider>,
        );

        expect(screen.getByTestId('custom-avatar')).toBeInTheDocument();
        expect(screen.queryByRole('img')).not.toBeInTheDocument();
        expect(screen.queryByText('JD')).not.toBeInTheDocument();
        expect(screen.getByText('Jane Doe')).toBeInTheDocument();
    });
});
