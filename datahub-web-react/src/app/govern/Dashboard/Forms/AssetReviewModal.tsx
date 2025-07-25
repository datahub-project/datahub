import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { Button, Text } from '@src/alchemy-components';
import { EmbeddedListSearchModal } from '@src/app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { MAX_COUNT_VAL } from '@src/app/searchV2/utils/constants';
import Loading from '@src/app/shared/Loading';
import { pluralize } from '@src/app/shared/textUtil';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';

import { AndFilterInput } from '@types';

const AssetReviewWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    align-items: flex-end;
    flex-direction: column;
`;

const StyledButton = styled(Button)`
    min-width: 125px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

type Props = {
    orFilters?: AndFilterInput[];
    maxSelectableAssets?: number;
};

export default function AssetReviewModal({ orFilters, maxSelectableAssets }: Props) {
    const [isModalVisible, setIsModalVisible] = useState(false);

    const searchFlags = { rewriteQuery: false };
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: { query: '*', orFilters, count: 0, convertToPredicate: true, searchFlags },
        },
        skip: !orFilters,
    });

    const totalCount = useMemo(() => data?.searchAcrossEntities?.total || 0, [data?.searchAcrossEntities?.total]);

    if (!orFilters) {
        return null;
    }

    return (
        <AssetReviewWrapper>
            <StyledButton variant="text" onClick={() => setIsModalVisible(true)}>
                {loading && (
                    <div>
                        <Loading height={14} marginTop={0} />
                    </div>
                )}
                {!loading && !!totalCount && (
                    <>
                        View {totalCount.toLocaleString()}
                        {totalCount === MAX_COUNT_VAL && '+'}&nbsp;{pluralize(totalCount, 'Asset')}
                    </>
                )}
                {!loading && !totalCount && <>No Assets Selected</>}
            </StyledButton>
            {maxSelectableAssets && (
                <Text size="sm" color="gray" colorLevel={1700}>
                    *The first {maxSelectableAssets.toLocaleString()} assets will be used by this action.
                </Text>
            )}
            {isModalVisible && (
                <EmbeddedListSearchModal
                    title="View Selected Assets"
                    height="600px"
                    fixedOrFilters={orFilters}
                    onClose={() => setIsModalVisible(false)}
                    convertToPredicate
                    searchFlags={searchFlags}
                />
            )}
        </AssetReviewWrapper>
    );
}
