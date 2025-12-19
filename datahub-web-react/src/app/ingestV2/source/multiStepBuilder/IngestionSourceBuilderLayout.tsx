import { Breadcrumb } from '@components';
import React, { useEffect, useMemo, useRef } from 'react';
import styled from 'styled-components';

import { VerticalDivider } from '@components/components/Breadcrumb/components';
import { BreadcrumbItem } from '@components/components/Breadcrumb/types';

import { AIChat } from '@app/ingestV2/source/multiStepBuilder/AIChat';
import { IngestionSourceBottomPanel } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBottomPanel';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
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
    const { getCurrentStep, state, steps, goToStep, isStepVisited } = useMultiStepContext<
        MultiStepSourceBuilderState,
        IngestionSourceFormStep
    >();
    const currentStep = useMemo(() => getCurrentStep(), [getCurrentStep]);
    const isEditing = state?.isEditing;

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
                    label: 'Manage Data Sources',
                    href: tabUrlMap[TabType.Sources],
                    separator: <VerticalDivider type="vertical" />,
                },
                {
                    label: isEditing ? 'Update Source' : 'Create Source',
                    separator: <VerticalDivider type="vertical" />,
                },
                ...breadCrumpStepItems,
            ]}
        />
    );

    return (
        <PageLayout
            title={currentStep?.label}
            subTitle={currentStep?.subTitle}
            rightPanelContent={currentStep?.hideRightPanel ? null : <AIChat />}
            bottomPanelContent={currentStep?.hideBottomPanel ? null : <IngestionSourceBottomPanel />}
            topBreadcrumb={breadCrumb}
        >
            <ContentWrapper ref={scrollContainerRef}>{children}</ContentWrapper>
        </PageLayout>
    );
}
