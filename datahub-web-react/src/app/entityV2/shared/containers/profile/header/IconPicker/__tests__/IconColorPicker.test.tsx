import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import IconColorPicker from '@app/entityV2/shared/containers/profile/header/IconPicker/IconColorPicker';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { EntityType, IconLibrary } from '@types';

const refetchMock = vi.fn();
const mutateMock = vi.fn();

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({
        urn: 'urn:li:domain:test',
        entityType: EntityType.Domain,
    }),
    useRefetch: () => refetchMock,
}));

vi.mock('@graphql/mutations.generated', () => ({
    useUpdateDisplayPropertiesMutation: () => [mutateMock],
}));

vi.mock('@app/entityV2/shared/containers/profile/header/IconPicker/IconPicker', () => ({
    ChatIconPicker: ({
        onIconPick,
        selectedIcon,
    }: {
        onIconPick: (icon: string) => void;
        selectedIcon?: string | null;
    }) => (
        <div>
            <span data-testid="staged-icon">{selectedIcon}</span>
            <button type="button" onClick={() => onIconPick('Shapes')}>
                pick-shapes
            </button>
        </div>
    ),
}));

vi.mock('@components', async () => {
    const actual = await vi.importActual<typeof import('@components')>('@components');
    return {
        ...actual,
        ColorPicker: () => <div data-testid="color-picker" />,
        Modal: ({
            open,
            title,
            children,
            buttons,
        }: {
            open: boolean;
            title: string;
            children: React.ReactNode;
            buttons: { text: string; onClick: () => void }[];
        }) =>
            open ? (
                <div>
                    <h1>{title}</h1>
                    {children}
                    {buttons.map((button) => (
                        <button key={button.text} type="button" onClick={button.onClick}>
                            {button.text}
                        </button>
                    ))}
                </div>
            ) : null,
        toast: { success: vi.fn(), error: vi.fn() },
    };
});

describe('IconColorPicker', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mutateMock.mockResolvedValue({ data: { updateDisplayProperties: true } });
    });

    it('maps a legacy Material icon into the staged pick and applies Phosphor + regular style', async () => {
        const user = userEvent.setup();
        const onClose = vi.fn();
        const onChangeIcon = vi.fn();

        render(
            <CustomThemeProvider>
                <IconColorPicker
                    name="Finance"
                    open
                    onClose={onClose}
                    icon="AccountCircle"
                    iconLibrary={IconLibrary.Material}
                    color="#abcdef"
                    onChangeIcon={onChangeIcon}
                />
            </CustomThemeProvider>,
        );

        expect(screen.getByTestId('staged-icon')).toHaveTextContent('UserCircle');

        await user.click(screen.getByRole('button', { name: 'pick-shapes' }));
        expect(screen.getByTestId('staged-icon')).toHaveTextContent('Shapes');

        await user.click(screen.getByRole('button', { name: /apply/i }));

        await waitFor(() => {
            expect(mutateMock).toHaveBeenCalledWith(
                expect.objectContaining({
                    variables: {
                        urn: 'urn:li:domain:test',
                        input: {
                            colorHex: '#abcdef',
                            icon: {
                                iconLibrary: IconLibrary.Phosphor,
                                name: 'Shapes',
                                style: 'regular',
                            },
                        },
                    },
                }),
            );
        });
        expect(onChangeIcon).toHaveBeenCalledWith('Shapes');
        expect(onClose).toHaveBeenCalled();
    });
});
