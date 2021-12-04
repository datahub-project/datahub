import { Modal, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { CreateScheduleStep } from './builder/CreateScheduleStep';
import { DefineRecipeStep } from './builder/DefineRecipeStep';
import { SelectTemplateStep } from './builder/SelectTemplateStep';
import { NameSourceStep } from './builder/NameSourceStep';
import { BaseBuilderState, IngestionSourceBuilderStep, StepProps } from './builder/types';

type Props = {
    initialState?: BaseBuilderState;
    visible: boolean;
    onSubmit?: (input: BaseBuilderState) => void;
    onCancel?: () => void;
};

/**
 * Mapping from the step type to the component implementing that step.
 */
export const IngestionSourceBuilderStepComponent = {
    SELECT_TEMPLATE: SelectTemplateStep,
    DEFINE_RECIPE: DefineRecipeStep,
    // CONFIGURE_TEMPLATE: ConfigureSourceTemplateStep,
    CREATE_SCHEDULE: CreateScheduleStep,
    NAME_SOURCE: NameSourceStep,
};

export const IngestionSourceBuilderModal = ({ initialState, visible, onSubmit, onCancel }: Props) => {
    console.log(initialState);
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? 'Edit Ingestion Source' : 'New Ingestion Source';
    const initialStep = isEditing
        ? IngestionSourceBuilderStep.DEFINE_RECIPE
        : IngestionSourceBuilderStep.SELECT_TEMPLATE;

    const [stepStack, setStepStack] = useState([initialStep]);
    useEffect(() => setStepStack([initialStep]), [initialState, initialStep, setStepStack]);

    const [ingestionBuilderState, setIngestionBuilderState] = useState<BaseBuilderState>({});
    useEffect(() => {
        if (initialState) {
            setIngestionBuilderState(initialState);
        }
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

    const StepComponent: React.FC<StepProps> = IngestionSourceBuilderStepComponent[stepStack[stepStack.length - 1]];

    return (
        <Modal
            width={800}
            footer={null}
            title={<Typography.Text>{titleText}</Typography.Text>}
            visible={visible}
            onCancel={onCancel}
        >
            <StepComponent
                state={ingestionBuilderState}
                updateState={setIngestionBuilderState}
                goTo={goTo}
                prev={stepStack.length > 1 ? prev : undefined}
                submit={() => onSubmit?.(ingestionBuilderState)}
                cancel={cancel}
            />
        </Modal>
    );
};
