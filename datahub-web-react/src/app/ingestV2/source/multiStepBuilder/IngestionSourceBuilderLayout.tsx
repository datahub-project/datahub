import React, { useCallback, useMemo } from 'react';

import { EmbeddedChat } from '@app/chat/EmbeddedChat';
import { MessageContext } from '@app/chat/hooks/useChatStream';
import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import { buildIngestionSourceChatContext } from '@app/ingestV2/source/multiStepBuilder/utils';
import { MultiStepFormBottomPanel } from '@app/sharedV2/forms/multiStepForm/MultiStepFormBottomPanel';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { PageLayout } from '@app/sharedV2/layouts/PageLayout';

import { DataHubAiConversationOriginType } from '@types';

interface Props {
    children: React.ReactNode;
    isEditing?: boolean;
    sourceUrn?: string;
}

export function IngestionSourceBuilderLayout({ children, isEditing = false, sourceUrn }: Props) {
    const { getCurrentStep, state } = useMultiStepContext<SourceBuilderState, IngestionSourceFormStep>();
    const step = useMemo(() => getCurrentStep(), [getCurrentStep]);

    // Create callback that generates fresh context on each message send
    const getMessageContext = useCallback((): MessageContext => {
        const contextText = buildIngestionSourceChatContext({
            isEditing,
            sourceUrn,
            sourceType: state?.type,
            sourceName: state?.name,
            currentStep: step?.label,
            stepContext: step?.context,
        });
        return { text: contextText };
    }, [isEditing, sourceUrn, state?.type, state?.name, step?.label, step?.context]);

    return (
        <PageLayout
            title={step?.label}
            rightPanelContent={
                step?.hideRightPanel ? null : (
                    <EmbeddedChat
                        context=""
                        agentName="IngestionTroubleshooter"
                        originType={DataHubAiConversationOriginType.DatahubUi}
                        title={isEditing ? 'Ask DataHub - Edit Source' : 'Ask DataHub - Create Source'}
                        getMessageContext={getMessageContext}
                    />
                )
            }
            bottomPanelContent={step?.hideBottomPanel ? null : <MultiStepFormBottomPanel />}
        >
            {children}
        </PageLayout>
    );
}
