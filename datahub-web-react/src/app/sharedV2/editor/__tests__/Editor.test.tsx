import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { EditorProps } from '@components/components/Editor/types';

import { Editor } from '@app/sharedV2/editor/Editor';
import { useIsDocumentationFileUploadV1Enabled } from '@app/sharedV2/hooks/useIsDocumentationFileUploadV1Enabled';

const mentionsUi = vi.hoisted(() => ({ fails: false, failedPicker: false, failedMentionView: false }));

vi.mock('@app/sharedV2/hooks/useIsDocumentationFileUploadV1Enabled', () => ({
    useIsDocumentationFileUploadV1Enabled: vi.fn(),
}));

// Renders what the wrapper plugs in, so the test sees it without loading Remirror.
vi.mock('@components/components/Editor', () => ({
    Editor: ({ mentionsPicker: Picker, mentionView: MentionView, imageUploadEnabled, content }: EditorProps) => {
        return (
            <>
                <div data-testid="content">{content}</div>
                <div data-testid="image-upload">{String(imageUploadEnabled)}</div>
                {Picker && <Picker renderOutsideEditor />}
                {MentionView && <MentionView urn="urn:li:corpuser:alice" name="Alice" />}
            </>
        );
    },
}));

vi.mock('@app/sharedV2/editor/mentions/MentionsComponent', () => ({
    MentionsComponent: ({ renderOutsideEditor }: { renderOutsideEditor?: boolean }) => {
        if (mentionsUi.fails) {
            mentionsUi.failedPicker = true;
            throw new Error('chunk failed to load');
        }
        return <div data-testid="mentions-picker" data-outside={String(!!renderOutsideEditor)} />;
    },
}));

vi.mock('@app/sharedV2/editor/mentions/MentionView', () => ({
    MentionView: ({ urn }: { urn: string }) => {
        if (mentionsUi.fails) {
            mentionsUi.failedMentionView = true;
            throw new Error('chunk failed to load');
        }
        return <span data-testid="mention-view">{urn}</span>;
    },
}));

function renderEditor(flag: boolean, { mentionsUiFails = false } = {}) {
    mentionsUi.fails = mentionsUiFails;
    mentionsUi.failedPicker = false;
    mentionsUi.failedMentionView = false;
    vi.mocked(useIsDocumentationFileUploadV1Enabled).mockReturnValue(flag);
    return render(<Editor content="Hello" />);
}

describe('Editor', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('passes the props through to the alchemy Editor', () => {
        renderEditor(false);

        expect(screen.getByTestId('content')).toHaveTextContent('Hello');
    });

    it('plugs in the DataHub mentions picker and mention view', async () => {
        renderEditor(false);

        expect(await screen.findByTestId('mentions-picker')).toHaveAttribute('data-outside', 'true');
        expect(await screen.findByTestId('mention-view')).toHaveTextContent('urn:li:corpuser:alice');
    });

    it('keeps the page up and shows the mention name when the mentions UI fails', async () => {
        // Silences the boundary's report; how it reports differs between OSS and DataHub Cloud.
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});

        renderEditor(false, { mentionsUiFails: true });

        // The Suspense placeholder shows the same name, so wait until the lazy components threw.
        await waitFor(() => expect(mentionsUi.failedPicker && mentionsUi.failedMentionView).toBe(true));
        expect(screen.getByText('Alice')).toBeInTheDocument();
        expect(screen.queryByTestId('mentions-picker')).not.toBeInTheDocument();
        expect(screen.getByTestId('content')).toBeInTheDocument();
    });

    it.each([true, false])('passes the documentation file upload flag (%s) as imageUploadEnabled', (flag) => {
        renderEditor(flag);

        expect(screen.getByTestId('image-upload')).toHaveTextContent(String(flag));
    });
});
