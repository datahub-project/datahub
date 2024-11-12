import { Button } from '@src/alchemy-components';
import React, { useContext, useMemo, useState } from 'react';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { MAX_COUNT_VAL } from '@src/app/searchV2/utils/constants';
import styled from 'styled-components';
import { pluralize } from '@src/app/shared/textUtil';
import Loading from '@src/app/shared/Loading';
import { EmbeddedListSearchModal } from '@src/app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import ManageFormContext from './ManageFormContext';

const AssetReviewWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const StyledButton = styled(Button)`
    min-width: 125px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

export default function AssetReviewModal() {
    const { formValues } = useContext(ManageFormContext);

    const [isModalVisible, setIsModalVisible] = useState(false);

    const orFilters = formValues.assets?.orFilters;

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
