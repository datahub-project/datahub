import { Alert } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { getViewAllSupport } from '@app/homeV3/modules/assetCollection/useAssetCollectionViewAll';
import LogicalFiltersBuilder from '@app/sharedV2/queryBuilder/LogicalFiltersBuilder';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { properties } from '@app/sharedV2/queryBuilder/properties';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

/** Start with one blank condition row so users can immediately pick a property. */
const DEFAULT_FILTER: LogicalPredicate = {
    type: 'logical',
    operator: LogicalOperatorType.AND,
    operands: [{ type: 'property' }],
};

const HintWrapper = styled.div`
    margin-top: 8px;
`;

type Props = {
    dynamicFilter: LogicalPredicate | null | undefined;
    setDynamicFilter: (newDynamicFilter: LogicalPredicate | null | undefined) => void;
};

const DynamicSelectAssetsTab = ({ dynamicFilter, setDynamicFilter }: Props) => {
    const { t } = useTranslation('modules');
    const entityRegistry = useEntityRegistryV2();

    const viewAllSupport = useMemo(
        () => getViewAllSupport(dynamicFilter, (name) => entityRegistry.getTypeFromGraphName(name)),
        [dynamicFilter, entityRegistry],
    );

    return (
        <>
            <LogicalFiltersBuilder
                filters={dynamicFilter ?? DEFAULT_FILTER}
                onChangeFilters={setDynamicFilter}
                properties={properties}
            />
            {viewAllSupport !== 'supported' && (
                <HintWrapper>
                    <Alert
                        variant="info"
                        title={t(
                            viewAllSupport === 'incomplete'
                                ? 'assetCollection.viewAllIncompleteHint'
                                : 'assetCollection.viewAllUnsupportedHint',
                        )}
                        data-testid="view-all-unsupported-hint"
                    />
                </HintWrapper>
            )}
        </>
    );
};

export default DynamicSelectAssetsTab;
