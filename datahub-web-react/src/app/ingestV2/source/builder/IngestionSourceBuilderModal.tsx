import { Modal } from '@components';
import { Steps } from 'antd';
import { isEqual } from 'lodash';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { CreateScheduleStep } from '@app/ingestV2/source/builder/CreateScheduleStep';
import { DefineRecipeStep } from '@app/ingestV2/source/builder/DefineRecipeStep';
import { NameSourceStep } from '@app/ingestV2/source/builder/NameSourceStep';
import { SelectTemplateStep } from '@app/ingestV2/source/builder/SelectTemplateStep';
import sourcesJson from '@app/ingestV2/source/builder/sources.json';
import { SourceBuilderState, StepProps } from '@app/ingestV2/source/builder/types';

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

const modalBodyStyle = { padding: '16px 24px 16px 24px', backgroundColor: '#F6F6F6' };

type Props = {
    initialState?: SourceBuilderState;
    open: boolean;
    onSubmit?: (input: SourceBuilderState, resetState: () => void, shouldRun?: boolean) => void;
    onCancel: () => void;
    sourceRefetch?: () => Promise<any>;
};

export const IngestionSourceBuilderModal = ({ initialState, open, onSubmit, onCancel, sourceRefetch }: Props) => {
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

    const ingestionSources = JSON.parse(JSON.stringify(sourcesJson)); // TODO: replace with call to server once we have access to dynamic list of sources

    // Reset the ingestion builder modal state when the modal is re-opened.
    const prevInitialState = useRef(initialState);
    useEffect(() => {
        if (!isEqual(prevInitialState.current, initialState)) {
            setIngestionBuilderState(initialState || {});
        }
        prevInitialState.current = initialState;
    }, [initialState]);

    // Reset the step stack to the initial step when the modal is re-opened.
    useEffect(() => setStepStack([initialStep]), [initialStep]);

    const goTo = (step: IngestionSourceBuilderStep) => {
        setStepStack([...stepStack, step]);
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
        <Modal
            width="64%"
            footer={null}
            title={titleText}
            style={{ top: 40 }}
            bodyStyle={modalBodyStyle}
            open={open}
            onCancel={onCancel}
            buttons={[]}
        >
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
            />
        </Modal>
    );
};
