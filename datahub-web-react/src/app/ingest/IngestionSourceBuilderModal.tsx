import { message, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { useCreateIngestionSourceMutation, useUpdateIngestionSourceMutation } from '../../graphql/ingestion.generated';
import { IngestionSourceType, UpdateIngestionSourceInput } from '../../types.generated';
import { CreateScheduleStep } from './builder/CreateScheduleStep';
import { DefineRecipeStep } from './builder/DefineRecipeStep';
import { SelectTemplateStep } from './builder/SelectTemplateStep';
import { NameSourceStep } from './builder/NameSourceStep';
import { BaseBuilderState, IngestionSourceBuilderStep, RecipeBuilderState, StepProps } from './builder/types';

type Props = {
    urn?: string;
    visible: boolean;
    onSubmit?: (input: UpdateIngestionSourceInput) => void;
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

export const IngestionSourceBuilderModal = ({ urn, visible, onSubmit, onCancel }: Props) => {
    const isEditing = urn !== undefined;
    const titleText = isEditing ? 'Edit Ingestion Source' : 'Create new Ingestion Source';

    // TODO: Support initial state.
    const [stepStack, setStepStack] = useState([IngestionSourceBuilderStep.SELECT_TEMPLATE]);
    const [ingestionBuilderState, setIngestionBuilderState] = useState<BaseBuilderState>({});

    const [createIngestionSource] = useCreateIngestionSourceMutation();
    const [updateIngestionSource] = useUpdateIngestionSourceMutation();

    const createOrUpdateIngestionSource = (input: UpdateIngestionSourceInput) => {
        if (isEditing) {
            // Update:
            updateIngestionSource({ variables: { urn: urn as string, input } })
                .then(() => {
                    message.success({
                        content: `Successfully updated ingestion source!`,
                        duration: 3,
                    });
                    onSubmit?.(input);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to update ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        } else {
            // Create:
            createIngestionSource({ variables: { input } })
                .then(() => {
                    message.success({
                        content: `Successfully created ingestion source!`,
                        duration: 3,
                    });
                    onSubmit?.(input);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to create ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        }
    };

    const goTo = (step: IngestionSourceBuilderStep) => {
        setStepStack([...stepStack, step]);
    };

    const prev = () => {
        setStepStack(stepStack.slice(0, -1));
    };

    const submit = () => {
        // 1. validateState()
        // 2. Create a new ingestion source.
        if (ingestionBuilderState.type === 'recipe') {
            const recipeBuilderState = ingestionBuilderState as RecipeBuilderState;
            createOrUpdateIngestionSource({
                type: IngestionSourceType.Recipe,
                displayName: recipeBuilderState.name || '', // Validate that this is not null.
                config: {
                    recipe: {
                        json: recipeBuilderState.recipe,
                    },
                },
                schedule: recipeBuilderState.schedule,
            });
        } else {
            throw new Error('Unrecognized builder type provided');
        }
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
                prev={prev}
                submit={submit}
            />
        </Modal>
    );
};
