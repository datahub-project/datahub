import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { AIChat } from '@app/ingestV2/source/multiStepBuilder/AIChat';
import { PageLayout } from '@app/sharedV2/layouts/PageLayout';
import { isHiddenRightPanel } from '@app/sharedV2/layouts/rightPanelContent';
import CustomThemeProvider from '@src/CustomThemeProvider';

function VisiblePanel() {
    return <div>visible panel</div>;
}

describe('PageLayout right panel', () => {
    it('renders right panel content', () => {
        render(
            <CustomThemeProvider>
                <PageLayout rightPanelContent={<VisiblePanel />} />
            </CustomThemeProvider>,
        );

        expect(screen.getByText('visible panel')).toBeInTheDocument();
    });

    it('recognizes the OSS AIChat stub as a hidden panel', () => {
        expect(isHiddenRightPanel(<AIChat />)).toBe(true);
        expect(isHiddenRightPanel(<VisiblePanel />)).toBe(false);
    });

    it('omits the rail for the OSS AIChat stub without mounting it', () => {
        render(
            <CustomThemeProvider>
                <PageLayout rightPanelContent={<AIChat />} />
            </CustomThemeProvider>,
        );

        expect(screen.queryByText('AI chat placeholder')).not.toBeInTheDocument();
    });
});
