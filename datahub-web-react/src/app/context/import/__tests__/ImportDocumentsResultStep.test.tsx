import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import ImportDocumentsResultStep from '@app/context/import/components/ImportDocumentsResultStep';
import CustomThemeProvider from '@src/CustomThemeProvider';

const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <CustomThemeProvider>{children}</CustomThemeProvider>
);

describe('ImportDocumentsResultStep', () => {
    it('renders import failure details', () => {
        render(
            <Wrapper>
                <ImportDocumentsResultStep
                    importError="Upload failed"
                    totalImported={0}
                    totalUpdated={0}
                    totalFailed={0}
                />
            </Wrapper>,
        );

        expect(screen.getByText('Import Failed')).toBeInTheDocument();
        expect(screen.getByText('Upload failed')).toBeInTheDocument();
    });

    it('renders combined success summary', () => {
        render(
            <Wrapper>
                <ImportDocumentsResultStep importError={null} totalImported={2} totalUpdated={1} totalFailed={0} />
            </Wrapper>,
        );

        expect(screen.getByText('Import Complete')).toBeInTheDocument();
        expect(screen.getByText('2 documents created, 1 document updated')).toBeInTheDocument();
    });

    it('renders empty summary when nothing changed', () => {
        render(
            <Wrapper>
                <ImportDocumentsResultStep importError={null} totalImported={0} totalUpdated={0} totalFailed={0} />
            </Wrapper>,
        );

        expect(screen.getByText('No documents imported')).toBeInTheDocument();
    });
});
