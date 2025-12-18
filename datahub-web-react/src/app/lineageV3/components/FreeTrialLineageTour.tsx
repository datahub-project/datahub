import { CalloutCard, Icon, Text, colors, typography } from '@components';
import React, { useContext, useMemo, useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { LineageDisplayContext } from '@app/lineageV3/common';
import { FREE_TRIAL } from '@app/onboarding/configV2/FreeTrialConfig';
import { useFreeTrialPopoverVisibility } from '@app/sharedV2/freeTrial';
import { PageRoutes } from '@conf/Global';
import { EducationStepsContext } from '@providers/EducationStepsContext';

import { useBatchUpdateStepStatesMutation } from '@graphql/step.generated';
import { StepStateResult } from '@types';

const BulletList = styled.ul`
    margin: 0;
    padding-left: 20px;
    color: ${colors.gray[1700]};
    font-size: ${typography.fontSizes.md};
    line-height: 1.6;

    li {
        margin-bottom: 4px;
    }
`;

const ParagraphContent = styled(Text)`
    color: ${colors.gray[1700]};
    line-height: 1.6;
`;

interface TourStep {
    id: string;
    icon: React.ReactNode;
    title: string;
    content: React.ReactNode;
    expandColumns?: boolean;
}

const TOUR_STEPS: TourStep[] = [
    {
        id: FREE_TRIAL.LINEAGE_TOUR_STEP_1_ID,
        icon: <Icon icon="TreeStructure" source="phosphor" color="violet" size="2xl" weight="fill" />,
        title: 'DataHub Lineage Features',
        content: (
            <ParagraphContent>
                Lineage helps you draw connections from sources, to warehouses, through transformations, and into
                business intelligence dashboards.
            </ParagraphContent>
        ),
    },
    {
        id: FREE_TRIAL.LINEAGE_TOUR_STEP_2_ID,
        icon: <Icon icon="TreeStructure" source="phosphor" color="violet" size="2xl" weight="fill" />,
        title: 'DataHub Lineage Features',
        content: (
            <BulletList>
                <li>Connect across platforms and tools</li>
                <li>Auto-extract lineage between sources</li>
                <li>View dependencies for instant impact analysis</li>
            </BulletList>
        ),
    },
    {
        id: FREE_TRIAL.LINEAGE_TOUR_STEP_3_ID,
        icon: <Icon icon="TreeStructure" source="phosphor" color="violet" size="2xl" weight="fill" />,
        title: 'Column-Level Lineage',
        expandColumns: true,
        content: (
            <BulletList>
                <li>Track PII for things like GDPR compliance</li>
                <li>Debug data quality issues to the exact column</li>
                <li>See which source fields feed your critical reports</li>
            </BulletList>
        ),
    },
    {
        id: FREE_TRIAL.LINEAGE_TOUR_STEP_4_ID,
        icon: <Icon icon="Confetti" source="phosphor" color="violet" size="2xl" weight="fill" />,
        title: "You've Seen Lineage!",
        content: (
            <ParagraphContent>Continue learning about DataHub next with Ask Datahub and our Sources.</ParagraphContent>
        ),
    },
];

const TOUR_STEP_IDS = [
    FREE_TRIAL.LINEAGE_TOUR_STEP_1_ID,
    FREE_TRIAL.LINEAGE_TOUR_STEP_2_ID,
    FREE_TRIAL.LINEAGE_TOUR_STEP_3_ID,
    FREE_TRIAL.LINEAGE_TOUR_STEP_4_ID,
];

interface Props {
    rootUrn: string;
}

/**
 * Component that shows a guided tour for free trial users viewing the lineage graph.
 * Only shown to free trial users who haven't completed the tour.
 */
export default function FreeTrialLineageTour({ rootUrn }: Props) {
    const history = useHistory();
    const { setTourExpandColumnsUrn } = useContext(LineageDisplayContext);
    const { setEducationSteps } = useContext(EducationStepsContext);
    const [currentStepIndex, setCurrentStepIndex] = useState(0);
    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    // Memoize to prevent infinite loop in useEffect
    const stepIds = useMemo(() => TOUR_STEP_IDS, []);
    const { isVisible, setIsVisible } = useFreeTrialPopoverVisibility({ stepIds });

    // Helper to mark all tour steps as seen
    const markAllStepsAsSeen = () => {
        const states = TOUR_STEP_IDS.map((id) => ({ id, properties: [] }));

        batchUpdateStepStates({ variables: { input: { states } } }).then(() => {
            const results: StepStateResult[] = TOUR_STEP_IDS.map((id) => ({ id, properties: [] }));
            setEducationSteps((existingSteps) => (existingSteps ? [...existingSteps, ...results] : results));
        });
    };

    const handleClose = () => {
        // Mark all steps as seen when closing the tour
        markAllStepsAsSeen();
        setTourExpandColumnsUrn(null);
        setIsVisible(false);
    };

    const handleNext = () => {
        if (currentStepIndex < TOUR_STEPS.length - 1) {
            const nextStepIndex = currentStepIndex + 1;
            const nextStep = TOUR_STEPS[nextStepIndex];

            // Expand columns if the next step requires it
            if (nextStep.expandColumns) {
                setTourExpandColumnsUrn(rootUrn);
            }

            setCurrentStepIndex(nextStepIndex);
        } else {
            // Last step - close the tour
            setIsVisible(false);
        }
    };

    const handleGoToHome = () => {
        // Mark all steps as seen when completing the tour
        markAllStepsAsSeen();
        setTourExpandColumnsUrn(null);
        setIsVisible(false);
        history.push(PageRoutes.ROOT);
    };

    if (!isVisible) {
        return null;
    }

    const currentStep = TOUR_STEPS[currentStepIndex];
    const isLastStep = currentStepIndex === TOUR_STEPS.length - 1;

    return (
        <CalloutCard
            icon={currentStep.icon}
            title={currentStep.title}
            position="fixed-bottom-center"
            size="md"
            primaryButtonText={isLastStep ? 'Go to Home' : 'Next'}
            onPrimaryClick={isLastStep ? handleGoToHome : handleNext}
            showCloseButton={!isLastStep}
            onClose={handleClose}
        >
            {currentStep.content}
        </CalloutCard>
    );
}
