import { Text } from '@components';
import { message } from 'antd';
import React from 'react';

import AssetReviewModal from '@app/govern/Dashboard/Forms/AssetReviewModal';
import LogicalFiltersBuilder from '@app/govern/Dashboard/Forms/filters/LogicalFiltersBuilder';
import {
    MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT,
    PREDICATE_PROPERTIES,
} from '@app/observe/shared/bulkCreate/constants';
import { validateAndTransformAssetSelectorFilters } from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.utils';
import { LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';
import { convertLogicalPredicateToOrFilters } from '@app/tests/builder/steps/definition/builder/utils';

type Props = {
    filters: LogicalPredicate;
    setFilters: (filters: LogicalPredicate) => void;
};

export const AssetsSelection = ({ filters, setFilters }: Props) => {
    return (
        <div>
            <Text size="lg" color="gray" colorLevel={600} weight="semiBold">
                Select the Datasets to bulk create assertions for...
            </Text>
            <Text size="md" color="gray" colorLevel={1700}>
                If more than {MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT.toLocaleString()} datasets are selected, only the
                first {MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT.toLocaleString()} will be processed. Contact support if
                you need to create more.
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
        </div>
    );
};
