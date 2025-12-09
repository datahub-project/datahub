import React, { useMemo } from 'react';

import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { AIChat } from '@app/ingestV2/source/multiStepBuilder/AIChat';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import { MultiStepFormBottomPanel } from '@app/sharedV2/forms/multiStepForm/MultiStepFormBottomPanel';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { PageLayout } from '@app/sharedV2/layouts/PageLayout';

interface Props {
    children: React.ReactNode;
}

export function IngestionSourceBuilderLayout({ children }: Props) {
    const { getCurrentStep } = useMultiStepContext<SourceBuilderState, IngestionSourceFormStep>();
    const step = useMemo(() => getCurrentStep(), [getCurrentStep]);

    return (
        <PageLayout
            title={step?.label}
            rightPanelContent={step?.hideRightPanel ? null : <AIChat />}
            bottomPanelContent={step?.hideBottomPanel ? null : <MultiStepFormBottomPanel />}
        >
            {children}
        </PageLayout>
    );
}
