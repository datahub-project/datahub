import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Editor } from '@components/components/Editor/EditorImpl';

import themeV2 from '@conf/theme/themeV2';

vi.mock('@components/components/Editor/toolbar/AddImageButton', () => ({
    AddImageButton: () => <div data-testid="image-by-url" />,
}));

vi.mock('@components/components/Editor/toolbar/AddImageButtonV2', () => ({
    AddImageButtonV2: () => <div data-testid="image-upload" />,
}));

const onFileUpload = async () => 'https://example.com/file.png';

function renderEditor(imageUploadEnabled: boolean | undefined, withUploadHandler = true) {
    return render(
        <ThemeProvider theme={themeV2}>
            <Editor
                content="Hello"
                doNotFocus
                imageUploadEnabled={imageUploadEnabled}
                uploadFileProps={withUploadHandler ? { onFileUpload } : undefined}
            />
        </ThemeProvider>,
    );
}

describe('Toolbar image button', () => {
    it('uploads images when imageUploadEnabled is set and the Editor has an upload handler', async () => {
        renderEditor(true);

        expect(await screen.findByTestId('image-upload')).toBeInTheDocument();
        expect(screen.queryByTestId('image-by-url')).not.toBeInTheDocument();
    });

    it('adds images by URL without imageUploadEnabled', async () => {
        renderEditor(undefined);

        expect(await screen.findByTestId('image-by-url')).toBeInTheDocument();
        expect(screen.queryByTestId('image-upload')).not.toBeInTheDocument();
    });

    it('adds images by URL when the Editor has no upload handler', async () => {
        renderEditor(true, false);

        expect(await screen.findByTestId('image-by-url')).toBeInTheDocument();
    });
});
