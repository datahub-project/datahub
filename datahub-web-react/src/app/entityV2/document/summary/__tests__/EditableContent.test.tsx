import { MockedProvider } from '@apollo/client/testing';
import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { EditableContent } from '@app/entityV2/document/summary/EditableContent';
import { AppConfigContext, DEFAULT_APP_CONFIG } from '@src/appConfigContext';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const { updateContents, updateRelatedEntities, mockPush } = vi.hoisted(() => ({
    updateContents: vi.fn().mockResolvedValue(true),
    updateRelatedEntities: vi.fn().mockResolvedValue(true),
    mockPush: vi.fn(),
}));

vi.mock('react-router-dom', async () => {
    const actual = await vi.importActual<typeof import('react-router-dom')>('react-router-dom');
    return {
        ...actual,
        useHistory: () => ({
            ...actual.useHistory(),
            push: mockPush,
        }),
    };
});

vi.mock('@components/components/Editor', () => ({
    Editor: ({
        content,
        onChange,
        readOnly,
        belowToolbar,
        ...rest
    }: {
        content?: string;
        onChange?: (value: string) => void;
        readOnly?: boolean;
        belowToolbar?: React.ReactNode;
        'data-testid'?: string;
    }) => (
        <>
            <textarea
                data-testid={rest['data-testid']}
                readOnly={readOnly}
                value={content ?? ''}
                onChange={(event) => onChange?.(event.target.value)}
            />
            {belowToolbar}
        </>
    ),
}));

vi.mock('@components', async () => {
    const actual = await vi.importActual<typeof import('@components')>('@components');
    return {
        ...actual,
        Editor: ({
            content,
            onChange,
            readOnly,
            belowToolbar,
            ...rest
        }: {
            content?: string;
            onChange?: (value: string) => void;
            readOnly?: boolean;
            belowToolbar?: React.ReactNode;
            'data-testid'?: string;
        }) => (
            <>
                <textarea
                    data-testid={rest['data-testid']}
                    readOnly={readOnly}
                    value={content ?? ''}
                    onChange={(event) => onChange?.(event.target.value)}
                />
                {belowToolbar}
            </>
        ),
    };
});

vi.mock('@app/document/hooks/useDocumentPermissions', () => ({
    useDocumentPermissions: () => ({
        canCreate: true,
        canEditContents: true,
        canEditTitle: true,
        canEditState: true,
        canEditType: true,
        canDelete: true,
        canMove: true,
    }),
}));

vi.mock('@app/document/hooks/useUpdateDocument', () => ({
    useUpdateDocument: () => ({
        updateContents,
        updateRelatedEntities,
    }),
}));

vi.mock('@app/sharedV2/hooks/useFileUpload', () => ({
    default: () => ({ uploadFile: vi.fn() }),
}));

function renderEditor(
    explicitSaveEnabled: boolean,
    acrylProps?: React.ComponentProps<typeof EditableContent>['acrylProps'],
) {
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <AppConfigContext.Provider
                    value={{
                        loaded: true,
                        refreshContext: () => undefined,
                        config: {
                            ...DEFAULT_APP_CONFIG,
                            featureFlags: {
                                ...DEFAULT_APP_CONFIG.featureFlags,
                                documentExplicitSaveEnabled: explicitSaveEnabled,
                            },
                        },
                    }}
                >
                    <EditableContent documentUrn="urn:li:document:1" initialContent="hello" acrylProps={acrylProps} />
                </AppConfigContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('EditableContent', () => {
    beforeEach(() => {
        updateContents.mockClear();
        updateRelatedEntities.mockClear();
        mockPush.mockClear();
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('auto-saves body edits when explicit save is off', async () => {
        renderEditor(false);

        fireEvent.change(screen.getByTestId('document-content-editor'), { target: { value: 'hello world' } });
        expect(screen.queryByTestId('document-save-bar')).not.toBeInTheDocument();

        await act(async () => {
            vi.advanceTimersByTime(3000);
        });

        expect(updateContents).toHaveBeenCalledWith({
            urn: 'urn:li:document:1',
            contents: { text: 'hello world' },
        });
    });

    it('keeps body edits local until Save when explicit save is on', async () => {
        renderEditor(true);

        fireEvent.change(screen.getByTestId('document-content-editor'), { target: { value: 'hello world' } });

        await act(async () => {
            vi.advanceTimersByTime(3000);
        });
        expect(updateContents).not.toHaveBeenCalled();
        expect(screen.getByTestId('document-save-bar')).toBeInTheDocument();

        fireEvent.click(screen.getByTestId('document-save-button'));

        expect(updateContents).toHaveBeenCalledWith({
            urn: 'urn:li:document:1',
            contents: { text: 'hello world' },
        });
    });

    it('discards unsaved edits on Cancel without saving', () => {
        renderEditor(true);

        fireEvent.change(screen.getByTestId('document-content-editor'), { target: { value: 'hello world' } });
        fireEvent.click(screen.getByTestId('document-cancel-button'));

        expect(updateContents).not.toHaveBeenCalled();
        expect(screen.queryByTestId('document-save-bar')).not.toBeInTheDocument();
        expect(screen.getByTestId('document-content-editor')).toHaveValue('hello');
    });

    it('renders additional save-bar actions when provided', () => {
        renderEditor(true, {
            renderSaveBarActions: () => <button type="button">Propose</button>,
        });

        fireEvent.change(screen.getByTestId('document-content-editor'), { target: { value: 'hello world' } });

        expect(screen.getByRole('button', { name: 'Propose' })).toBeInTheDocument();
        expect(screen.getByTestId('document-save-button')).toBeInTheDocument();
    });

    it('navigates to same-origin asset links on click when viewing', () => {
        renderEditor(false);

        const editorSection = screen.getByTestId('document-editor-section');
        const link = document.createElement('a');
        link.href = '/dataset/urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)';
        link.textContent = 'Linked Asset';
        editorSection.appendChild(link);

        fireEvent.mouseDown(link);
        fireEvent.click(link);

        expect(mockPush).toHaveBeenCalledWith('/dataset/urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)');
        expect(screen.queryByTestId('document-save-bar')).not.toBeInTheDocument();
    });

    it('opens external links in a new tab when viewing', () => {
        const windowOpenSpy = vi.spyOn(window, 'open').mockImplementation(() => null);
        renderEditor(false);

        const editorSection = screen.getByTestId('document-editor-section');
        const link = document.createElement('a');
        link.href = 'https://example.com/external-doc';
        link.textContent = 'External Link';
        editorSection.appendChild(link);

        fireEvent.mouseDown(link);
        fireEvent.click(link);

        expect(windowOpenSpy).toHaveBeenCalledWith('https://example.com/external-doc', '_blank', 'noopener,noreferrer');
        windowOpenSpy.mockRestore();
    });

    it('does not navigate on simple click when editor is focused for editing', () => {
        renderEditor(false);

        const editorSection = screen.getByTestId('document-editor-section');
        const link = document.createElement('a');
        link.href = '/dataset/urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)';
        link.textContent = 'Linked Asset';
        editorSection.appendChild(link);

        // Focus the editor first (e.g. user clicked textarea to edit)
        fireEvent.focus(editorSection);

        fireEvent.mouseDown(link);
        fireEvent.click(link);

        expect(mockPush).not.toHaveBeenCalled();
    });
});
