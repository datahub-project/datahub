import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import CreateNewTagModal from '@app/tags/CreateNewTagModal/CreateNewTagModal';
import themeV2 from '@conf/theme/themeV2';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const { createTag, setTagColor, addOwners, addTags } = vi.hoisted(() => ({
    createTag: vi.fn(),
    setTagColor: vi.fn(),
    addOwners: vi.fn(),
    addTags: vi.fn(),
}));

vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
    initReactI18next: { type: '3rdParty', init: vi.fn() },
}));

vi.mock('@app/domainV2/OwnersSection', () => ({ default: () => null }));
vi.mock('@utils/test-utils/TestPageContainer', () => ({
    default: ({ children }: { children: React.ReactNode }) => <ThemeProvider theme={themeV2}>{children}</ThemeProvider>,
}));
vi.mock('@components', async () => ({
    toast: {
        error: vi.fn(),
        success: vi.fn(),
        destroy: vi.fn(),
    },
    Modal: ({
        children,
        buttons,
    }: {
        children: React.ReactNode;
        buttons: import('@components/components/Modal/Modal').ModalButton[];
    }) => (
        <div>
            {children}
            {buttons.map((button) => (
                <button
                    key={button.text}
                    type="button"
                    data-testid={button.buttonDataTestId}
                    disabled={button.disabled}
                    onClick={button.onClick}
                >
                    {button.text}
                </button>
            ))}
        </div>
    ),
    Input: ({
        label,
        value,
        setValue,
        error,
        ...props
    }: {
        label: string;
        value: string;
        setValue: (value: string) => void;
        error?: string;
        'data-testid'?: string;
    }) => (
        <label htmlFor={props['data-testid']}>
            {label}
            <input
                id={props['data-testid']}
                data-testid={props['data-testid']}
                value={value}
                onChange={(event) => setValue(event.target.value)}
            />
            {error && <span>{error}</span>}
        </label>
    ),
    ColorPicker: () => null,
}));
vi.mock('@app/entityV2/shared/utils/selectorUtils', () => ({ createOwnerInputs: vi.fn() }));
vi.mock('@app/shared/useEnterKeyListener', () => ({ useEnterKeyListener: vi.fn() }));
vi.mock('@graphql/tag.generated', () => ({ useCreateTagMutation: () => [createTag] }));
vi.mock('@graphql/mutations.generated', () => ({
    useSetTagColorMutation: () => [setTagColor],
    useBatchAddOwnersMutation: () => [addOwners],
    useBatchAddTagsMutation: () => [addTags],
}));

function renderModal() {
    render(
        <TestPageContainer>
            <CreateNewTagModal open onClose={vi.fn()} initialTagName="My Tag" />
        </TestPageContainer>,
    );
}

describe('CreateNewTagModal custom ID', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        createTag.mockResolvedValue({ data: { createTag: 'urn:li:tag:new-tag' } });
        setTagColor.mockResolvedValue({});
    });

    it('omits a blank ID so the server generates a UUID', async () => {
        renderModal();
        fireEvent.click(screen.getByTestId('create-tag-modal-create-button'));

        await waitFor(() =>
            expect(createTag).toHaveBeenCalledWith({
                variables: { input: { id: undefined, name: 'My Tag', description: '' } },
            }),
        );
    });

    it('submits the custom suffix without changing the display name', async () => {
        renderModal();
        fireEvent.click(screen.getByText('tags.advanced'));
        fireEvent.change(screen.getByTestId('tag-id-field'), { target: { value: 'my.tag-1' } });
        fireEvent.click(screen.getByTestId('create-tag-modal-create-button'));

        await waitFor(() =>
            expect(createTag).toHaveBeenCalledWith({
                variables: { input: { id: 'my.tag-1', name: 'My Tag', description: '' } },
            }),
        );
    });

    it.each(['urn:li:tag:full-urn', 'has space', ',leading', '(paren)', 'has\ttab', ')leading'])(
        'blocks invalid suffix %j',
        (suffix) => {
            renderModal();
            fireEvent.click(screen.getByText('tags.advanced'));
            fireEvent.change(screen.getByTestId('tag-id-field'), { target: { value: suffix } });

            expect(screen.getByText('tags.idInvalid')).toBeInTheDocument();
            expect(screen.getByTestId('create-tag-modal-create-button')).toBeDisabled();
            expect(createTag).not.toHaveBeenCalled();
        },
    );
});
