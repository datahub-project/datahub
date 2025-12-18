import { Input, spacing, transition } from '@components';
import { Form } from 'antd';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { AntdFormCompatibleCheckbox } from '@app/ingestV2/source/multiStepBuilder/components/AntdCompatibleCheckbox';
import { ExpandCollapseButton } from '@app/ingestV2/source/multiStepBuilder/components/ExpandCollapseButton';
import { SectionName } from '@app/ingestV2/source/multiStepBuilder/components/SectionName';
import { MAX_FORM_WIDTH } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/constants';
import { CustomLabelFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/CustomFormItem';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.sm};
`;

const FormContainer = styled(Container)<{ $expanded?: boolean }>`
    max-width: ${MAX_FORM_WIDTH};
    max-height: ${(props) => (props.$expanded ? '100%' : '0px')};
    transform-origin: top;
    opacity: ${(props) => (props.$expanded ? 1 : 0)};
    transform: ${(props) => (props.$expanded ? 'scaleY(1)' : 'scaleY(0)')};
    transition:
        transform ${transition.duration.normal} ${transition.easing['ease-in-out']},
        opacity ${transition.duration.normal} ${transition.easing['ease-in-out']};
`;

interface Props {
    state: MultiStepSourceBuilderState;
    updateState: (newState: Partial<MultiStepSourceBuilderState>) => void;
}

const ExtraEnvKey = 'extra_env_vars';
const ExtraReqKey = 'extra_pip_requirements';
const ExtraPluginKey = 'extra_pip_plugins';

export function AdvancedSection({ state, updateState }: Props) {
    const [isExpanded, setIsExpanded] = useState<boolean>(false);

    const toggleIsExpanded = useCallback(() => {
        setIsExpanded((prev) => !prev);
    }, []);

    function retrieveValueFromExtraArgs(builderState: MultiStepSourceBuilderState, key: string) {
        return builderState.config?.extraArgs?.find((entry) => entry.key === key)?.value ?? '';
    }

    const initialState = useMemo(() => {
        return {
            executor_id: state.config?.executorId ?? '',
            cli_version: state.config?.version ?? '',
            debug_mode: state.config?.debugMode ?? '',
            extra_args: retrieveValueFromExtraArgs(state, ExtraEnvKey),
            extra_pip_plugin: retrieveValueFromExtraArgs(state, ExtraPluginKey),
            extra_pip_reqs: retrieveValueFromExtraArgs(state, ExtraReqKey),
        };
    }, [state]);

    const form = useFormInstance();

    const onValuesChange = useCallback(
        (_, values) => {
            updateState({
                ...state,
                config: {
                    ...state.config,
                    executorId: values.executor_id,
                    version: values.cli_version,
                    debugMode: values.debug_mode,
                    extraArgs: [
                        { key: ExtraEnvKey, value: values.extra_args },
                        { key: ExtraPluginKey, value: values.extra_pip_plugin },
                        { key: ExtraReqKey, value: values.extra_pip_reqs },
                    ],
                },
            });
        },
        [updateState, state],
    );

    return (
        <Form form={form} layout="vertical" initialValues={initialState} onValuesChange={onValuesChange}>
            <Container>
                <SectionName
                    name="Advanced Settings"
                    topRowRightItems={<ExpandCollapseButton expanded={isExpanded} onToggle={toggleIsExpanded} />}
                />

                <FormContainer $expanded={isExpanded}>
                    {/* NOTE: Executor ID is OSS-only, used by actions pod */}
                    <CustomLabelFormItem
                        label="Executor ID"
                        help="Provide the ID of the executor that should execute this ingestion recipe. This ID is used to route
                    execution requests of the recipe to the executor of the same ID. The built-in DataHub executor ID is
                    'default'. Do not change this unless you have configured a custom executor via actions
                    framework."
                        name="executor_id"
                    >
                        <Input placeholder="default" value={state.config?.executorId || ''} />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem
                        label="CLI Version"
                        help="Advanced: Provide a custom CLI version to use for ingestion."
                        name="cli_version"
                    >
                        <Input data-testid="cli-version-input" placeholder="(e.g. 0.15.0)" />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem label="Debug Mode" name="debug_mode">
                        <AntdFormCompatibleCheckbox
                            checked={state.config?.debugMode || false}
                            helper="Advanced: Turn on debug mode in order to get more verbose logs."
                        />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem
                        label="Extra Enviroment Variables"
                        help="Advanced: Set extra environment variables to an ingestion execution"
                        name="extra_args"
                    >
                        <Input data-testid="extra-args-input" placeholder='{"MY_CUSTOM_ENV": "my_custom_value2"}' />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem
                        label="Extra DataHub plugins"
                        help="Advanced: Set extra DataHub plugins for an ingestion execution"
                        name="extra_pip_plugin"
                    >
                        <Input data-testid="extra-pip-plugin-input" placeholder='["debug"]' />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem
                        label="Extra Pip Libraries"
                        help="Advanced: Add extra pip libraries for an ingestion execution"
                        name="extra_pip_reqs"
                    >
                        <Input data-testid="extra-pip-reqs-input" placeholder='["sqlparse==0.4.3"]' />
                    </CustomLabelFormItem>
                </FormContainer>
            </Container>
        </Form>
    );
}
