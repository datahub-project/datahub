import { Assertion, AssertionType, EntityType } from '@src/types.generated';
import { Form, Steps } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { useConnectionForEntityExists } from '../../acrylUtils';
import { AssertionBuilderStepTitles, getAssertionsBuilderStepComponent } from './conf';
import { DEFAULT_BUILDER_STATE } from './constants';
import getInitBuilderStateByAssertionType from './steps/utils';
import { AssertionBuilderStep, AssertionMonitorBuilderState, StepProps } from './types';
import { useUpsertAssertionMonitor } from './useUpsertAssertionMonitor';
import { isEntityEligibleForAssertionMonitoring } from './utils';

const MainContent = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
`;

const StepsContainer = styled.div`
    margin-bottom: 20px;
`;

const StyledForm = styled(Form)`
    flex: 1;
`;

const stepIds = Object.values(AssertionBuilderStep);

type Props = {
    entityUrn: string;
    entityType: EntityType;
    platformUrn: string;
    initialState?: AssertionMonitorBuilderState;
    onSubmit?: (assertion: Assertion) => void;
    onCancel?: () => void;
    predefinedType?: AssertionType;
};

export const AssertionMonitorBuilder = ({
    entityUrn,
    entityType,
    platformUrn,
    initialState,
    onSubmit,
    onCancel,
    predefinedType,
}: Props) => {
    const [form] = Form.useForm();
    const [assertionType, setAssertionType] = useState(AssertionType.Freshness);
    const [stepStack, setStepStack] = useState<AssertionBuilderStep[]>([AssertionBuilderStep.SELECT_TYPE]);
    const [builderState, setBuilderState] = useState<AssertionMonitorBuilderState>(
        initialState || { ...DEFAULT_BUILDER_STATE, entityUrn, entityType, platformUrn },
    );
    const [isInitializedWithPredefinedType, setIsInitializedWithPredefinedType] = useState<boolean>(false);

    const connectionForEntityExists = useConnectionForEntityExists(builderState.entityUrn as string);
    const isConnectionSupportedByMonitors = isEntityEligibleForAssertionMonitoring(builderState.platformUrn);
    const monitorsConnectionForEntityExists = connectionForEntityExists && isConnectionSupportedByMonitors;

    const steps = useMemo(() => {
        // Skip the first step if the type is predefined
        if (predefinedType) return stepIds.slice(1);
        return stepIds;
    }, [predefinedType]);

    const stepIndexToId = useMemo(() => {
        const map = new Map();
        steps.forEach((stepId, index) => map.set(stepId, index));
        return map;
    }, [steps]);

    useEffect(() => {
        if (predefinedType && !isInitializedWithPredefinedType) {
            setAssertionType(predefinedType);

            // Initialize state for `Configure Assertion` step
            setBuilderState((prevState) =>
                getInitBuilderStateByAssertionType(
                    prevState,
                    predefinedType,
                    connectionForEntityExists,
                    monitorsConnectionForEntityExists,
                ),
            );

            setStepStack([AssertionBuilderStep.CONFIGURE_ASSERTION]);
            setIsInitializedWithPredefinedType(true);
        }
    }, [
        predefinedType,
        setBuilderState,
        setIsInitializedWithPredefinedType,
        isInitializedWithPredefinedType,
        connectionForEntityExists,
        monitorsConnectionForEntityExists,
    ]);

    const onCreateAssertionMonitor = (newAssertion: Assertion) => {
        onSubmit?.(newAssertion);
        setBuilderState(DEFAULT_BUILDER_STATE);
    };

    const createAssertionMonitor = useUpsertAssertionMonitor(builderState, onCreateAssertionMonitor, false);

    /**
     * The current step id, e.g. SELECT_TYPE
     */
    const currentStep = stepStack[stepStack.length - 1];
    /**
     * The current step index, e.g. 0
     */
    const currentStepIndex = stepIndexToId.get(currentStep);
    /**
     * The current step component
     */
    const StepComponent: React.FC<StepProps> = getAssertionsBuilderStepComponent(currentStep, assertionType);

    const validateForm = async (shouldRetry?: boolean) => {
        try {
            await form.validateFields();
            return true;
        } catch (e) {
            console.warn('Validate Failed:', e);
            if (shouldRetry && (e as any)?.outOfDate) {
                // Cypress tests run into this error... seems harmless enough to retry once
                return validateForm(false);
            }
            return false;
        }
    };

    const goTo = async (step: AssertionBuilderStep, type?: AssertionType, shouldValidate = true) => {
        if (shouldValidate) {
            const isValid = await validateForm(true);
            if (!isValid) return;
        }
        if (type) setAssertionType(type);
        setStepStack([...stepStack, step]);
    };

    const handleSubmit = async () => {
        const isValid = await validateForm();
        if (!isValid) return;
        await createAssertionMonitor();
    };

    const prev = () => {
        setStepStack(stepStack.slice(0, -1));
    };

    const cancel = () => {
        onCancel?.();
    };

    return (
        <MainContent>
            <StepsContainer>
                <Steps current={currentStepIndex}>
                    {steps.map((id) => (
                        <Steps.Step key={id} title={AssertionBuilderStepTitles[id]} />
                    ))}
                </Steps>
            </StepsContainer>
            <StyledForm form={form} initialValues={initialState}>
                <StepComponent
                    state={builderState}
                    updateState={setBuilderState}
                    goTo={goTo}
                    prev={stepStack.length > 1 ? prev : undefined}
                    submit={handleSubmit}
                    cancel={cancel}
                />
            </StyledForm>
        </MainContent>
    );
};
