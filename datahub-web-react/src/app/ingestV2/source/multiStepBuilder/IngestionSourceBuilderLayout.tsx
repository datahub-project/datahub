import React, { useMemo } from 'react';
import styled from 'styled-components';

import { AIChat } from '@app/ingestV2/source/multiStepBuilder/AIChat';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { MultiStepFormBottomPanel } from '@app/sharedV2/forms/multiStepForm/MultiStepFormBottomPanel';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { PageLayout } from '@app/sharedV2/layouts/PageLayout';

const ContentWrapper = styled.div`
    padding: 0 20px 16px 20px;
    overflow: auto;
    height: 100%;
`;

interface Props {
    children: React.ReactNode;
}

export function IngestionSourceBuilderLayout({ children }: Props) {
    const { getCurrentStep } = useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep>();
    const step = useMemo(() => getCurrentStep(), [getCurrentStep]);

    return (
        <PageLayout
            title={step?.label}
            subTitle={step?.subTitle}
            rightPanelContent={step?.hideRightPanel ? null : <AIChat />}
            bottomPanelContent={step?.hideBottomPanel ? null : <MultiStepFormBottomPanel />}
        >
            <ContentWrapper>{children}</ContentWrapper>
        </PageLayout>
    );
}
