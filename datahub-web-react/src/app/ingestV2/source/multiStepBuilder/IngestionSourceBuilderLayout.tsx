import { Breadcrumb } from '@components';
import React, { useCallback, useEffect, useMemo, useRef } from 'react';
import styled from 'styled-components';

import { VerticalDivider } from '@components/components/Breadcrumb/components';
import { BreadcrumbItem } from '@components/components/Breadcrumb/types';

import { EmbeddedChat } from '@app/chat/EmbeddedChat';
import { MessageContext } from '@app/chat/hooks/useChatStream';
import { IngestionSourceBottomPanel } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBottomPanel';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { buildIngestionSourceChatContext } from '@app/ingestV2/source/multiStepBuilder/utils';
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
    const currentStep = useMemo(() => getCurrentStep(), [getCurrentStep]);

    const scrollContainerRef = useRef<HTMLDivElement>(null);

    const breadCrumpStepItems: BreadcrumbItem[] = steps.map((step) => {
        const breadCrumpItem: BreadcrumbItem = {
            label: step.label,
            onClick: isStepVisited(step.key) ? () => goToStep(step.key) : undefined,
        };

        return breadCrumpItem;
    }, []);

    useEffect(() => {
        if (scrollContainerRef.current) {
            scrollContainerRef.current.scrollTo({ top: 0 });
        }
    }, [currentStep]);

    const breadCrumb = (
        <Breadcrumb
            items={[
                {
                    label: isEditing ? 'Update Source' : 'Create Source',
                    separator: <VerticalDivider type="vertical" />,
                },
                ...breadCrumpStepItems,
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
        });
        return { text: contextText };
    }, [isEditing, sourceUrn, state?.type, state?.name, currentStep?.label, currentStep?.context]);

    return (
        <PageLayout
            title={currentStep?.label}
            subTitle={currentStep?.subTitle}
            rightPanelContent={
                currentStep?.hideRightPanel ? null : (
                    <EmbeddedChat
                        context=""
                        agentName="IngestionTroubleshooter"
                        originType={DataHubAiConversationOriginType.DatahubUi}
                        title={isEditing ? 'Ask DataHub - Edit Source' : 'Ask DataHub - Create Source'}
                        getMessageContext={getMessageContext}
                    />
                )
            }
            bottomPanelContent={currentStep?.hideBottomPanel ? null : <IngestionSourceBottomPanel />}
            topBreadcrumb={breadCrumb}
        >
            <ContentWrapper ref={scrollContainerRef}>{children}</ContentWrapper>
        </PageLayout>
    );
}
