import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import FileUploadSource from '@app/context/import/sources/FileUploadSource';
import CustomThemeProvider from '@src/CustomThemeProvider';

const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <CustomThemeProvider>{children}</CustomThemeProvider>
);

function createFile(name: string, size = 100): File {
    const content = new Uint8Array(size);
    return new File([content], name, { type: 'application/octet-stream' });
}

describe('FileUploadSource', () => {
    it('renders supported formats text', () => {
        render(
            <Wrapper>
                <FileUploadSource files={[]} onFilesChange={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.getByText(/Supported formats:/)).toBeInTheDocument();
        expect(screen.getByText(/Very long files may be truncated/)).toBeInTheDocument();
    });

    it('displays selected files', () => {
        const files = [createFile('readme.md'), createFile('guide.txt')];

        render(
            <Wrapper>
                <FileUploadSource files={files} onFilesChange={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.getByText('readme.md')).toBeInTheDocument();
        expect(screen.getByText('guide.txt')).toBeInTheDocument();
        expect(screen.getByText('2 files selected')).toBeInTheDocument();
    });

    it('displays singular text for one file', () => {
        const files = [createFile('doc.md')];

        render(
            <Wrapper>
                <FileUploadSource files={files} onFilesChange={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.getByText('1 file selected')).toBeInTheDocument();
    });
});
