import { Button, Modal, Steps, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { ExpandAltOutlined, ShrinkOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { CreateScheduleStep } from './CreateScheduleStep';
import { DefineRecipeStep } from './DefineRecipeStep';
import { SelectTemplateStep } from './SelectTemplateStep';
import { NameSourceStep } from './NameSourceStep';
import { SourceBuilderState, IngestionSourceBuilderStep, StepProps } from './types';

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

type Props = {
    initialState?: SourceBuilderState;
    visible: boolean;
    onSubmit?: (input: SourceBuilderState, resetState: () => void) => void;
    onCancel?: () => void;
};

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
 * Mapping from the step type to the title for the step
 */
export enum IngestionSourceBuilderStepTitles {
    SELECT_TEMPLATE = 'Choose Type',
    DEFINE_RECIPE = 'Configure Recipe',
    CREATE_SCHEDULE = 'Schedule Execution',
    NAME_SOURCE = 'Finish up',
}

export const IngestionSourceBuilderModal = ({ initialState, visible, onSubmit, onCancel }: Props) => {
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? 'Edit Ingestion Source' : 'New Ingestion Source';
    const initialStep = isEditing
        ? IngestionSourceBuilderStep.DEFINE_RECIPE
        : IngestionSourceBuilderStep.SELECT_TEMPLATE;

    const [stepStack, setStepStack] = useState([initialStep]);
    useEffect(() => setStepStack([initialStep]), [initialState, initialStep, setStepStack]);

    const [modalExpanded, setModalExpanded] = useState(false);

    const [ingestionBuilderState, setIngestionBuilderState] = useState<SourceBuilderState>({});
    useEffect(() => {
        setIngestionBuilderState(initialState || {});
    }, [initialState, setIngestionBuilderState]);

    const goTo = (step: IngestionSourceBuilderStep) => {
        setStepStack([...stepStack, step]);
    };

    const prev = () => {
        setStepStack(stepStack.slice(0, -1));
    };

    const cancel = () => {
        onCancel?.();
    };

    const submit = () => {
        onSubmit?.(ingestionBuilderState, () => {
            setStepStack([initialStep]);
            setIngestionBuilderState({});
        });
    };

    const currentStep = stepStack[stepStack.length - 1];
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
                <Steps current={stepStack.length - 1}>
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
            />
        </Modal>
    );
};
