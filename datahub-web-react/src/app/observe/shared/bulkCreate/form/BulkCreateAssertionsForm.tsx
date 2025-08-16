import { Button } from '@components';
import { Divider, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { BulkCreateDatasetAssertionsSpec } from '@app/observe/shared/bulkCreate/constants';
import { DEFAULT_ASSET_SELECTOR_FILTERS } from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.constants';
import { stateToBulkCreateDatasetAssertionsSpec } from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.utils';
import { AssertionsConfiguration } from '@app/observe/shared/bulkCreate/form/steps/AssertionsConfiguration';
import { AssetsSelection } from '@app/observe/shared/bulkCreate/form/steps/AssetsSelection';
import { SubscriptionConfiguration } from '@app/observe/shared/bulkCreate/form/steps/SubscriptionConfiguration';
import { useFreshnessForm } from '@app/observe/shared/bulkCreate/form/useFreshnessForm';
import { useSubscriptionsForm } from '@app/observe/shared/bulkCreate/form/useSubscriptionsForm';
import { useVolumeForm } from '@app/observe/shared/bulkCreate/form/useVolumeForm';
import { LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const CreateButton = styled(Button)`
    align-self: flex-end;
`;

const ActionButtons = styled.div`
    display: flex;
    gap: 8px;
    justify-content: flex-end;
`;

type Steps = 'asset_selection' | 'assertion_configuration' | 'subscription_configuration';
const ASSET_SELECTION_STEP: Steps = 'asset_selection';
const ASSERTION_CONFIGURATION_STEP: Steps = 'assertion_configuration';
const SUBSCRIPTION_CONFIGURATION_STEP: Steps = 'subscription_configuration';

type Props = {
    onSubmit: (spec: BulkCreateDatasetAssertionsSpec) => void;
};

export const BulkCreateAssertionsForm = ({ onSubmit }: Props) => {
    const [step, setStep] = useState<Steps>(ASSET_SELECTION_STEP);
    const onNextStep = () => {
        switch (step) {
            case ASSET_SELECTION_STEP:
                setStep(ASSERTION_CONFIGURATION_STEP);
                break;
            case ASSERTION_CONFIGURATION_STEP:
                setStep(SUBSCRIPTION_CONFIGURATION_STEP);
                break;
            default:
            // Do nothing.
        }
    };
    const onPrevStep = () => {
        switch (step) {
            case SUBSCRIPTION_CONFIGURATION_STEP:
                setStep(ASSERTION_CONFIGURATION_STEP);
                break;
            case ASSERTION_CONFIGURATION_STEP:
                setStep(ASSET_SELECTION_STEP);
                break;
            default:
            // Do nothing.
        }
    };

    // --------------------------------- Asset Selector state --------------------------------- //
    const [filters, setFilters] = React.useState<LogicalPredicate>(DEFAULT_ASSET_SELECTOR_FILTERS);

    const selectedPlatformOperand = filters.operands.find(
        (operand) => operand.type === 'property' && operand.property === 'platform',
    );
    const selectedPlatformUrn: string | undefined =
        selectedPlatformOperand?.type === 'property' ? selectedPlatformOperand?.values?.[0] : undefined;

    const isPlatformSelected = selectedPlatformUrn !== undefined;

    // --------------------------------- Freshness Assertion state --------------------------------- //
    const { component: freshnessForm, state: freshnessFormState } = useFreshnessForm({
        selectedPlatformUrn,
    });
    const { freshnessAssertionEnabled, freshnessSourceType } = freshnessFormState;

    // --------------------------------- Volume Assertion state --------------------------------- //
    const { component: volumeForm, state: volumeFormState } = useVolumeForm({
        selectedPlatformUrn,
    });
    const { volumeAssertionEnabled, volumeSourceType } = volumeFormState;

    // --------------------------------- Subscription state --------------------------------- //
    const { component: subscriptionForm, state: subscriptionFormState } = useSubscriptionsForm();

    // --------------------------------- Event Handlers --------------------------------- //
    const onCreateAssertions = () => {
        if (freshnessAssertionEnabled && !freshnessSourceType) {
            message.warn('Please select a freshness source to enable freshness assertions.');
            return;
        }
        if (volumeAssertionEnabled && !volumeSourceType) {
            message.warn('Please select a volume source to enable volume assertions.');
            return;
        }
        onSubmit(
            stateToBulkCreateDatasetAssertionsSpec({
                filters,
                freshnessFormState,
                volumeFormState,
                subscriptionFormState,
            }),
        );
    };

    // --------------------------------- Render UI --------------------------------- //
    return (
        <Wrapper style={{ display: 'flex', flexDirection: 'column' }}>
            {/* --------------------------------- Asset Selector --------------------------------- */}
            {step === 'asset_selection' && <AssetsSelection filters={filters} setFilters={setFilters} />}

            {step === 'assertion_configuration' && (
                <AssertionsConfiguration freshnessForm={freshnessForm} volumeForm={volumeForm} />
            )}

            {step === 'subscription_configuration' && <SubscriptionConfiguration subscriptionForm={subscriptionForm} />}

            <Divider />

            <ActionButtons>
                <Button onClick={onPrevStep} variant="outline">
                    Back
                </Button>
                {step === 'asset_selection' && (
                    <Button
                        onClick={() => {
                            if (isPlatformSelected) {
                                onNextStep();
                            } else {
                                message.warn('Please select a platform to continue.');
                            }
                        }}
                    >
                        Next
                    </Button>
                )}
                {step === 'assertion_configuration' && (
                    <Button
                        onClick={() => {
                            if (freshnessAssertionEnabled || volumeAssertionEnabled) {
                                onNextStep();
                            } else {
                                message.warn('Please enable at least one assertion type to continue.');
                            }
                        }}
                    >
                        Next
                    </Button>
                )}
                {step === 'subscription_configuration' && (
                    <CreateButton onClick={onCreateAssertions}>Create Assertions</CreateButton>
                )}
            </ActionButtons>
        </Wrapper>
    );
};
