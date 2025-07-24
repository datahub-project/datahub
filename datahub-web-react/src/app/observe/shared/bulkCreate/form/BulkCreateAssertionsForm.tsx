import { Button, Text, colors } from '@components';
import { Alert, Divider, message } from 'antd';
import { Info } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import AssetReviewModal from '@app/govern/Dashboard/Forms/AssetReviewModal';
import LogicalFiltersBuilder from '@app/govern/Dashboard/Forms/filters/LogicalFiltersBuilder';
import {
    BulkCreateDatasetAssertionsSpec,
    MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT,
    PREDICATE_PROPERTIES,
} from '@app/observe/shared/bulkCreate/constants';
import { DEFAULT_ASSET_SELECTOR_FILTERS } from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.constants';
import {
    stateToBulkCreateDatasetAssertionsSpec,
    validateAndTransformAssetSelectorFilters,
} from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.utils';
import { useFreshnessForm } from '@app/observe/shared/bulkCreate/form/useFreshnessForm';
import { useVolumeForm } from '@app/observe/shared/bulkCreate/form/useVolumeForm';
import { LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';
import { convertLogicalPredicateToOrFilters } from '@app/tests/builder/steps/definition/builder/utils';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const CreateButton = styled(Button)`
    align-self: flex-end;
`;

type Props = {
    onSubmit: (spec: BulkCreateDatasetAssertionsSpec) => void;
};

export const BulkCreateAssertionsForm = ({ onSubmit }: Props) => {
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
            <Text size="lg" color="gray" colorLevel={600} weight="semiBold">
                Select the Datasets to bulk create assertions for...
            </Text>
            <Text size="md" color="gray" colorLevel={1700}>
                Max {MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT.toLocaleString()} datasets can be selected for a single
                run. Contact support if you need to create more.
            </Text>
            <LogicalFiltersBuilder
                filters={filters}
                onChangeFilters={(newFilters) => {
                    try {
                        const transformedFilters = validateAndTransformAssetSelectorFilters(newFilters);
                        if (transformedFilters) {
                            setFilters(transformedFilters);
                        }
                    } catch (error) {
                        if (error instanceof Error) {
                            message.warn(error.message);
                        } else {
                            message.warn('An unknown error occurred while validating the filters.');
                        }
                    }
                }}
                properties={PREDICATE_PROPERTIES}
            />
            <AssetReviewModal
                orFilters={convertLogicalPredicateToOrFilters(filters)}
                maxSelectableAssets={MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT}
            />

            <Divider />

            {/* --------------------------------- Freshness Assertion --------------------------------- */}
            {freshnessForm}
            <Divider />

            {/* --------------------------------- Volume Assertion --------------------------------- */}
            {volumeForm}

            <Divider />

            <Alert
                message={
                    <div>
                        <Text size="md" color="gray" colorLevel={600} weight="semiBold">
                            More assertion types
                        </Text>
                        <Text size="sm" color="gray" colorLevel={1700}>
                            For all available assertion types, use the &apos;Quality&apos; tab on individual dataset
                            pages.
                        </Text>
                    </div>
                }
                type="warning"
                icon={<Info color={colors.gray[600]} size={16} />}
                style={{ marginTop: 16 }}
                showIcon
            />

            <Divider />

            <CreateButton onClick={onCreateAssertions} disabled={!freshnessAssertionEnabled && !volumeAssertionEnabled}>
                Create Assertions
            </CreateButton>
        </Wrapper>
    );
};
