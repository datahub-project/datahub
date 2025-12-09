import React, { useMemo } from 'react';

import { EmbeddedChat } from '@app/chat/EmbeddedChat';
import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import { MultiStepFormBottomPanel } from '@app/sharedV2/forms/multiStepForm/MultiStepFormBottomPanel';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { PageLayout } from '@app/sharedV2/layouts/PageLayout';

import { DataHubAiConversationOriginType } from '@types';

interface Props {
    children: React.ReactNode;
}

export function IngestionSourceBuilderLayout({ children }: Props) {
    const { getCurrentStep } = useMultiStepContext<SourceBuilderState, IngestionSourceFormStep>();
    const step = useMemo(() => getCurrentStep(), [getCurrentStep]);

    return (
        <PageLayout
            title={step?.label}
            rightPanelContent={
                step?.hideRightPanel ? null : (
                    <EmbeddedChat
                        context=""
                        originType={DataHubAiConversationOriginType.DatahubUi}
                        title="Ask DataHub - Source Builder"
                    />
                )
            }
            bottomPanelContent={step?.hideBottomPanel ? null : <MultiStepFormBottomPanel />}
        >
            {children}
        </PageLayout>
    );
}
