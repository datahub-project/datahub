import { Breadcrumb } from '@components';
import React, { useEffect, useMemo, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { VerticalDivider } from '@components/components/Breadcrumb/components';
import { BreadcrumbItemType } from '@components/components/Breadcrumb/types';

import { AIChat } from '@app/ingestV2/source/multiStepBuilder/AIChat';
import IngestionSourceNavigationButtons from '@app/ingestV2/source/multiStepBuilder/IngestionSourceNavigationButtons';
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
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { getCurrentStep, state, steps, goToStep, isStepVisited } = useMultiStepContext<
        MultiStepSourceBuilderState,
        IngestionSourceFormStep
    >();
    const currentStep = useMemo(() => getCurrentStep(), [getCurrentStep]);
    const isEditing = state?.isEditing;

    const scrollContainerRef = useRef<HTMLDivElement>(null);

    const breadCrumbStepItems: BreadcrumbItemType[] = steps.map((step) => {
        const breadCrumbItem: BreadcrumbItemType = {
            key: step.key,
            label: step.label,
            onClick: isStepVisited(step.key) ? () => goToStep(step.key) : undefined,
            isActive: currentStep === step,
        };

        return breadCrumbItem;
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
                    key: 'back',
                    label: t('multiStep.builder.breadcrumbManageDataSources'),
                    href: tabUrlMap[TabType.Sources],
                    separator: <VerticalDivider type="vertical" />,
                },
                {
                    key: 'action',
                    label: isEditing
                        ? t('multiStep.builder.breadcrumbUpdateSource')
                        : t('multiStep.builder.breadcrumbCreateSource'),
                    separator: <VerticalDivider type="vertical" />,
                },
                ...breadCrumbStepItems,
            ]}
        />
    );

    return (
        <PageLayout
            title={currentStep?.label}
            subTitle={currentStep?.subTitle}
            rightPanelContent={currentStep?.hideRightPanel ? null : <AIChat />}
            topBreadcrumb={breadCrumb}
            topRightContent={<IngestionSourceNavigationButtons />}
        >
            <ContentWrapper ref={scrollContainerRef}>{children}</ContentWrapper>
        </PageLayout>
    );
}
