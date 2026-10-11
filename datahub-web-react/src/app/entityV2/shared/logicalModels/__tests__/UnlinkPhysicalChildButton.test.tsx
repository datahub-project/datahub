import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import UnlinkPhysicalChildButton from '@app/entityV2/shared/logicalModels/UnlinkPhysicalChildButton';
import themeV2 from '@conf/theme/themeV2';

const CHILD = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,c,PROD)';

const { unlink } = vi.hoisted(() => ({ unlink: vi.fn() }));

vi.mock('@graphql/logical.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/logical.generated')>();
    return {
        ...actual,
        useUnlinkPhysicalChildMutation: () => [unlink],
    };
});

const onUnlinked = vi.fn();

function renderButton() {
    render(
        <ThemeProvider theme={themeV2}>
            <UnlinkPhysicalChildButton childUrn={CHILD} onUnlinked={onUnlinked} />
        </ThemeProvider>,
    );
}

describe('UnlinkPhysicalChildButton', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        unlink.mockResolvedValue({ errors: undefined });
    });

    it('does not unlink until the action is confirmed', () => {
        renderButton();
        expect(screen.getByTestId('unlink-physical-child')).toBeInTheDocument();
        expect(unlink).not.toHaveBeenCalled();
    });

    it('unlinks the child after confirmation', async () => {
        renderButton();
        fireEvent.click(screen.getByTestId('unlink-physical-child'));
        fireEvent.click(await screen.findByTestId('modal-confirm-button'));

        await waitFor(() => expect(unlink).toHaveBeenCalledTimes(1));
        expect(unlink).toHaveBeenCalledWith({
            variables: { input: { childUrn: CHILD } },
        });
    });
});
