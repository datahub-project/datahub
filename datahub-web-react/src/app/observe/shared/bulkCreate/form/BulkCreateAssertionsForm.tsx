import { Button } from '@components';
import { Divider, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { BulkCreateDatasetAssertionsSpec } from '@app/observe/shared/bulkCreate/constants';
import { DEFAULT_ASSET_SELECTOR_FILTERS } from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.constants';
import { stateToBulkCreateDatasetAssertionsSpec } from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.utils';
import { AssertionsConfiguration } from '@app/observe/shared/bulkCreate/form/steps/AssertionsConfiguration';
import { AssetsSelection } from '@app/observe/shared/bulkCreate/form/steps/AssetsSelection';
import { useFreshnessForm } from '@app/observe/shared/bulkCreate/form/useFreshnessForm';
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

type Steps = 'asset_selection' | 'assertion_configuration';
const ASSET_SELECTION_STEP: Steps = 'asset_selection';
const ASSERTION_CONFIGURATION_STEP: Steps = 'assertion_configuration';

type Props = {
    onSubmit: (spec: BulkCreateDatasetAssertionsSpec) => void;
};

export const BulkCreateAssertionsForm = ({ onSubmit }: Props) => {
    const [step, setStep] = useState<Steps>(ASSET_SELECTION_STEP);
    const onNextStep = () => {
        setStep(step === ASSET_SELECTION_STEP ? ASSERTION_CONFIGURATION_STEP : ASSERTION_CONFIGURATION_STEP);
    };
    const onPrevStep = () => {
        setStep(step === ASSERTION_CONFIGURATION_STEP ? ASSET_SELECTION_STEP : ASSET_SELECTION_STEP);
    };

    // --------------------------------- Asset Selector state --------------------------------- //
    const [filters, setFilters] = React.useState<LogicalPredicate>(DEFAULT_ASSET_SELECTOR_FILTERS);

    const selectedPlatformOperand = filters.operands.find(
        (operand) => operand.type === 'property' && operand.property === 'platform',
    );
    const selectedPlatformUrn: string | undefined =
        selectedPlatformOperand?.type === 'property' ? selectedPlatformOperand?.values?.[0] : undefined;

    const canEnableAssertions = selectedPlatformUrn !== undefined;

    // --------------------------------- Freshness Assertion state --------------------------------- //
    const { component: freshnessForm, state: freshnessFormState } = useFreshnessForm({
        selectedPlatformUrn,
        canEnableAssertions,
    });
    const { freshnessAssertionEnabled, freshnessSourceType } = freshnessFormState;

    // --------------------------------- Volume Assertion state --------------------------------- //
    const { component: volumeForm, state: volumeFormState } = useVolumeForm({
        selectedPlatformUrn,
        canEnableAssertions,
    });
    const { volumeAssertionEnabled, volumeSourceType } = volumeFormState;

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

            <Divider />

            <ActionButtons>
                <Button onClick={onPrevStep} variant="outline">
                    Back
                </Button>
                {step === 'asset_selection' && <Button onClick={onNextStep}>Next</Button>}
                {step === 'assertion_configuration' && (
                    <CreateButton
                        onClick={onCreateAssertions}
                        disabled={!freshnessAssertionEnabled && !volumeAssertionEnabled}
                    >
                        Create Assertions
                    </CreateButton>
                )}
            </ActionButtons>
        </Wrapper>
    );
};
