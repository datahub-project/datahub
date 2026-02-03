import { LoadingOutlined } from '@ant-design/icons';
import { Modal } from '@components';
import { Spin, Steps } from 'antd';
import { isEqual } from 'lodash';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { CreateScheduleStep } from '@app/ingestV2/source/builder/CreateScheduleStep';
import { DefineRecipeStep } from '@app/ingestV2/source/builder/DefineRecipeStep';
import { NameSourceStep } from '@app/ingestV2/source/builder/NameSourceStep';
import { SelectTemplateStep } from '@app/ingestV2/source/builder/SelectTemplateStep';
import { SourceBuilderState, StepProps } from '@app/ingestV2/source/builder/types';
import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';

import { IngestionSource } from '@types';

const StepsContainer = styled.div`
    margin-right: 20px;
    margin-left: 20px;
    margin-bottom: 40px;
`;

/**
 * Mapping from the step type to the title for the step
 */
export enum IngestionSourceBuilderStepTitles {
    SELECT_TEMPLATE = 'Choose Data Source',
    DEFINE_RECIPE = 'Configure Connection',
    CREATE_SCHEDULE = 'Sync Schedule',
    NAME_SOURCE = 'Finish up',
}

/**
 * Mapping from the step type to the component implementing that step.
 */
export const IngestionSourceBuilderStepComponent = {
    SELECT_TEMPLATE: SelectTemplateStep,
    DEFINE_RECIPE: DefineRecipeStep,
    CREATE_SCHEDULE: CreateScheduleStep,
    NAME_SOURCE: NameSourceStep,
};

/**
 * Steps of the Ingestion Source Builder flow.
 */
export enum IngestionSourceBuilderStep {
    SELECT_TEMPLATE = 'SELECT_TEMPLATE',
    DEFINE_RECIPE = 'DEFINE_RECIPE',
    CREATE_SCHEDULE = 'CREATE_SCHEDULE',
    NAME_SOURCE = 'NAME_SOURCE',
}

type Props = {
    initialState?: SourceBuilderState;
    open: boolean;
    onSubmit?: (input: SourceBuilderState, resetState: () => void, shouldRun?: boolean) => void;
    onCancel: () => void;
    sourceRefetch?: () => Promise<any>;
    selectedSource?: IngestionSource;
    loading?: boolean;
    selectedSourceType?: string;
    setSelectedSourceType?: (sourceType: string) => void;
};

export const IngestionSourceBuilderModal = ({
    initialState,
    open,
    onSubmit,
    onCancel,
    sourceRefetch,
    selectedSource,
    loading,
    selectedSourceType,
    setSelectedSourceType,
}: Props) => {
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? 'Edit Data Source' : 'Connect Data Source';
    const initialStep = isEditing
        ? IngestionSourceBuilderStep.DEFINE_RECIPE
        : IngestionSourceBuilderStep.SELECT_TEMPLATE;

    const [stepStack, setStepStack] = useState([initialStep]);
    const [ingestionBuilderState, setIngestionBuilderState] = useState<SourceBuilderState>({
        schedule: {
            interval: '0 0 * * *',
        },
    });

    const { ingestionSources } = useIngestionSources();

    const sendAnalyticsStepViewedEvent = useCallback(
        (step: IngestionSourceBuilderStep) => {
            if (open) {
                analytics.event({
                    type: EventType.IngestionSourceConfigurationImpressionEvent,
                    viewedSection: step,
                    sourceType: selectedSource?.type || selectedSourceType,
                    sourceUrn: selectedSource?.urn,
                });
            }
        },
        [selectedSource?.type, selectedSource?.urn, selectedSourceType, open],
    );

    // Reset the modal state when initialState changes or modal opens
    const prevInitialState = useRef(initialState);
    const prevOpen = useRef(open);
    useEffect(() => {
        const stateChanged = !isEqual(prevInitialState.current, initialState);
        const modalOpened = !prevOpen.current && open;

        if (stateChanged) {
            setIngestionBuilderState(initialState || {});
            setStepStack([initialStep]);
            setSelectedSourceType?.('');
            prevInitialState.current = initialState;
        }

        // Fire event when modal opens
        if (modalOpened) {
            setStepStack([initialStep]); // Ensure correct step when modal opens
            sendAnalyticsStepViewedEvent(initialStep);
        }

        prevOpen.current = open;
    }, [initialState, initialStep, open, sendAnalyticsStepViewedEvent, setSelectedSourceType]);

    const goTo = (step: IngestionSourceBuilderStep) => {
        setStepStack([...stepStack, step]);
        sendAnalyticsStepViewedEvent(step);
    };

    const prev = () => {
        setStepStack(stepStack.slice(0, -1));
    };

    const cancel = () => {
        onCancel?.();
    };

    const submit = (shouldRun?: boolean) => {
        onSubmit?.(
            ingestionBuilderState,
            () => {
                setStepStack([initialStep]);
                setIngestionBuilderState({});
            },
            shouldRun,
        );
    };

    const currentStep = stepStack[stepStack.length - 1];
    const currentStepIndex = Object.values(IngestionSourceBuilderStep)
        .map((value, index) => ({
            value,
            index,
        }))
        .filter((obj) => obj.value === currentStep)[0].index;
    const StepComponent: React.FC<StepProps> = IngestionSourceBuilderStepComponent[currentStep];

    return (
        <Modal width="64%" title={titleText} open={open} onCancel={onCancel} buttons={[]}>
            <Spin spinning={loading} indicator={<LoadingOutlined />}>
                {currentStepIndex > 0 ? (
                    <StepsContainer>
                        <Steps current={currentStepIndex}>
                            {Object.keys(IngestionSourceBuilderStep).map((item) => (
                                <Steps.Step key={item} title={IngestionSourceBuilderStepTitles[item]} />
                            ))}
                        </Steps>
                    </StepsContainer>
                ) : null}
                <StepComponent
                    state={ingestionBuilderState}
                    updateState={setIngestionBuilderState}
                    isEditing={isEditing}
                    goTo={goTo}
                    prev={stepStack.length > 1 ? prev : undefined}
                    submit={submit}
                    cancel={cancel}
                    ingestionSources={ingestionSources}
                    sourceRefetch={sourceRefetch}
                    selectedSource={selectedSource}
                    selectedSourceType={selectedSourceType}
                    setSelectedSourceType={setSelectedSourceType}
                />
            </Spin>
        </Modal>
    );
};
