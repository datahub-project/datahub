import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import CreateGroupModal from '@app/identity/group/CreateGroupModal';
import theme from '@src/alchemy-components/theme';

vi.mock('react-i18next', () => ({
    useTranslation: () => ({
        t: (key: string) => key,
    }),
}));

vi.mock('@app/analytics', () => ({
    default: { event: vi.fn() },
    EventType: {},
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({ urn: 'urn:li:corpuser:testuser' }),
}));

vi.mock('@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect', () => ({
    ActorsSearchSelect: () => null,
}));

vi.mock('@app/shared/textUtil', () => ({
    validateCustomUrnId: () => true,
}));

vi.mock('@app/shared/useEnterKeyListener', () => ({
    useEnterKeyListener: () => {},
}));

vi.mock('@graphql/group.generated', () => ({
    useCreateGroupMutation: () => [vi.fn()],
    useAddGroupMembersMutation: () => [vi.fn()],
}));

vi.mock('@graphql/mutations.generated', () => ({
    useAddOwnerMutation: () => [vi.fn()],
}));

vi.mock('antd', async (importOriginal) => {
    const actual = await importOriginal<typeof import('antd')>();
    return { ...actual, message: { success: vi.fn(), error: vi.fn() } };
});

function wrap(ui: React.ReactElement) {
    return render(<ThemeProvider theme={theme as any}>{ui}</ThemeProvider>);
}

describe('CreateGroupModal name length', () => {
    it('accepts a name of exactly 250 characters', () => {
        wrap(<CreateGroupModal onClose={vi.fn()} onCreate={vi.fn()} />);
        const input = screen.getByTestId('group-name-input');

        fireEvent.change(input, { target: { value: 'a'.repeat(250) } });

        expect(screen.queryByText('groups.createModal.name.validationError.tooLong')).not.toBeInTheDocument();
    });

    it('rejects a name of 251 characters', () => {
        wrap(<CreateGroupModal onClose={vi.fn()} onCreate={vi.fn()} />);
        const input = screen.getByTestId('group-name-input');

        fireEvent.change(input, { target: { value: 'a'.repeat(251) } });

        expect(screen.getByText('groups.createModal.name.validationError.tooLong')).toBeInTheDocument();
    });

    it('enforces maxLength=250 on the input element', () => {
        wrap(<CreateGroupModal onClose={vi.fn()} onCreate={vi.fn()} />);
        const input = screen.getByTestId('group-name-input');

        expect(input).toHaveAttribute('maxlength', '250');
    });
});
