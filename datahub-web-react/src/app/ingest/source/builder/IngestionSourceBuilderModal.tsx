import { Button, Modal, Steps, Typography } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';
import { isEqual } from 'lodash';
import { ExpandAltOutlined, ShrinkOutlined } from '@ant-design/icons';
import { SourceBuilderState, StepProps } from './types';
import { CreateScheduleStep } from './CreateScheduleStep';
import { DefineRecipeStep } from './DefineRecipeStep';
import { NameSourceStep } from './NameSourceStep';
import { SelectTemplateStep } from './SelectTemplateStep';
import sourcesJson from './sources.json';

const ExpandButton = styled(Button)`
    && {
        margin-right: 32px;
    }
`;

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const StepsContainer = styled.div`
    margin-right: 20px;
    margin-left: 20px;
    margin-bottom: 40px;
`;

/**
 * Mapping from the step type to the title for the step
 */
export enum IngestionSourceBuilderStepTitles {
    SELECT_TEMPLATE = 'Choose Type',
    DEFINE_RECIPE = 'Configure Recipe',
    CREATE_SCHEDULE = 'Schedule Execution',
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
    visible: boolean;
    onSubmit?: (input: SourceBuilderState, resetState: () => void, shouldRun?: boolean) => void;
    onCancel?: () => void;
};

export const IngestionSourceBuilderModal = ({ initialState, visible, onSubmit, onCancel }: Props) => {
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? 'Edit Ingestion Source' : 'New Ingestion Source';
    const initialStep = isEditing
        ? IngestionSourceBuilderStep.DEFINE_RECIPE
        : IngestionSourceBuilderStep.SELECT_TEMPLATE;

    const [stepStack, setStepStack] = useState([initialStep]);
    const [modalExpanded, setModalExpanded] = useState(false);
    const [ingestionBuilderState, setIngestionBuilderState] = useState<SourceBuilderState>({});

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
            width={modalExpanded ? 1400 : 800}
            footer={null}
            title={
                <TitleContainer>
                    <Typography.Text>{titleText}</Typography.Text>
                    <ExpandButton onClick={() => setModalExpanded(!modalExpanded)}>
                        {(modalExpanded && <ShrinkOutlined />) || <ExpandAltOutlined />}
                    </ExpandButton>
                </TitleContainer>
            }
            style={{ top: 40 }}
            visible={visible}
            onCancel={onCancel}
        >
            <StepsContainer>
                <Steps current={currentStepIndex}>
                    {Object.keys(IngestionSourceBuilderStep).map((item) => (
                        <Steps.Step key={item} title={IngestionSourceBuilderStepTitles[item]} />
                    ))}
                </Steps>
            </StepsContainer>
            <StepComponent
                state={ingestionBuilderState}
                updateState={setIngestionBuilderState}
                goTo={goTo}
                prev={stepStack.length > 1 ? prev : undefined}
                submit={submit}
                cancel={cancel}
                ingestionSources={ingestionSources}
            />
        </Modal>
    );
};
