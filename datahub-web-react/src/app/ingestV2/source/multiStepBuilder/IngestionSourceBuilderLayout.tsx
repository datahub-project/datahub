import { Breadcrumb } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { VerticalDivider } from '@components/components/Breadcrumb/components';
import { BreadcrumbItem } from '@components/components/Breadcrumb/types';

import analytics, { EventType } from '@app/analytics';
import CollapsibleChat from '@app/chat/CollapsibleChat';
import FloatingChatButton from '@app/chat/FloatingChatButton';
import { MessageContext } from '@app/chat/hooks/useChatStream';
import { IngestionSourceBottomPanel } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBottomPanel';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { buildIngestionSourceChatContext } from '@app/ingestV2/source/multiStepBuilder/utils';
import { CUSTOM_SOURCE_DISPLAY_NAME } from '@app/ingestV2/source/utils';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { PageLayout } from '@app/sharedV2/layouts/PageLayout';

import { DataHubAiConversationOriginType } from '@types';

const ContentWrapper = styled.div`
    padding: 0 20px 16px 20px;
    overflow: auto;
    height: 100%;
`;

interface Props {
    children: React.ReactNode;
    isEditing?: boolean;
    sourceUrn?: string;
}

export function IngestionSourceBuilderLayout({ children, isEditing = false, sourceUrn }: Props) {
    const { getCurrentStep, state, steps, goToStep, isStepVisited } = useMultiStepContext<
        MultiStepSourceBuilderState,
        IngestionSourceFormStep
    >();
    const history = useHistory();
    const currentStep = useMemo(() => getCurrentStep(), [getCurrentStep]);

    const scrollContainerRef = useRef<HTMLDivElement>(null);

    const [isChatOpen, setIsChatOpen] = useState(true);

    const breadCrumbStepItems: BreadcrumbItem[] = steps.map((step) => {
        const breadCrumbItem: BreadcrumbItem = {
            label: step.label,
            onClick: isStepVisited(step.key) ? () => goToStep(step.key) : undefined,
            isCurrent: currentStep === step,
        };

        return breadCrumbItem;
    }, []);

    useEffect(() => {
        if (scrollContainerRef.current) {
            scrollContainerRef.current.scrollTo({ top: 0 });
        }
    }, [currentStep]);

    const goToDataSources = () => {
        history.push(tabUrlMap[TabType.Sources]);
        analytics.event({
            type: EventType.IngestionExitConfigurationEvent,
            exitType: 'abandon',
        });
    };

    const breadCrumb = (
        <Breadcrumb
            items={[
                {
                    label: 'Manage Data Sources',
                    separator: <VerticalDivider type="vertical" />,
                    onClick: goToDataSources,
                },
                {
                    label: isEditing ? 'Update Source' : 'Create Source',
                    separator: <VerticalDivider type="vertical" />,
                },
                ...breadCrumbStepItems,
            ]}
        />
    );

    // Create callback that generates fresh context on each message send
    const getMessageContext = useCallback((): MessageContext => {
        const contextText = buildIngestionSourceChatContext({
            isEditing,
            sourceUrn,
            sourceType: state?.type,
            sourceName: state?.name,
            currentStep: currentStep?.label,
            stepContext: currentStep?.context,
            recipe: state?.config?.recipe,
            executorId: state?.config?.executorId,
            version: state?.config?.version,
            debugMode: state?.config?.debugMode,
            extraArgs: state?.config?.extraArgs,
        });
        return { text: contextText };
    }, [
        isEditing,
        sourceUrn,
        state?.type,
        state?.name,
        state?.config?.recipe,
        state?.config?.executorId,
        state?.config?.version,
        state?.config?.debugMode,
        state?.config?.extraArgs,
        currentStep?.label,
        currentStep?.context,
    ]);

    return (
        <PageLayout
            title={currentStep?.label}
            subTitle={currentStep?.subTitle}
            rightPanelContent={
                currentStep?.hideRightPanel ? null : (
                    <CollapsibleChat
                        setIsChatOpen={setIsChatOpen}
                        context=""
                        agentName="IngestionTroubleshooter"
                        originType={DataHubAiConversationOriginType.IngestionUi}
                        title={isEditing ? 'Ask DataHub - Edit Source' : 'Ask DataHub - Create Source'}
                        getMessageContext={getMessageContext}
                        chatLocation="ingestion_configure_source"
                        contentPlaceholder={`Ask DataHub about ${state?.platformDisplayName === CUSTOM_SOURCE_DISPLAY_NAME ? 'this source' : state?.platformDisplayName || 'your data source'}`}
                    />
                )
            }
            bottomPanelContent={currentStep?.hideBottomPanel ? null : <IngestionSourceBottomPanel />}
            topBreadcrumb={breadCrumb}
            isRightPanelCollapsed={!isChatOpen}
        >
            <ContentWrapper ref={scrollContainerRef}>
                <>
                    {children}
                    <FloatingChatButton isVisible={!isChatOpen} onButtonClick={() => setIsChatOpen(true)} />
                </>
            </ContentWrapper>
        </PageLayout>
    );
}
