import { render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { urlEncodeUrn } from '@app/entity/shared/utils';
import { UserListItem, UserNameCell } from '@app/identity/user/UserListV2.components';
import theme from '@src/alchemy-components/theme';

import { EntityType } from '@types';

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntityUrl: (_type: EntityType, urn: string) => `/user/${urlEncodeUrn(urn)}`,
    }),
}));

function makeUser(overrides: Partial<UserListItem> = {}): UserListItem {
    return {
        __typename: 'CorpUser' as const,
        urn: 'urn:li:corpuser:jdoe@example.com',
        type: EntityType.CorpUser,
        username: 'jdoe@example.com',
        info: null,
        editableProperties: null,
        ...overrides,
    } as UserListItem;
}

describe('UserNameCell', () => {
    it('should prefix the user link with the router basename', () => {
        const user = makeUser();

        render(
            <ThemeProvider theme={theme as any}>
                <BrowserRouter basename="/datahub">
                    <UserNameCell user={user} />
                </BrowserRouter>
            </ThemeProvider>,
        );

        expect(screen.getByRole('link')).toHaveAttribute('href', `/datahub/user/${urlEncodeUrn(user.urn)}`);
    });
});
