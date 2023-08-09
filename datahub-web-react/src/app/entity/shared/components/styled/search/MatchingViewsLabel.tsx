import React from 'react';
import { Button, Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import { useListGlobalViewsQuery, useListMyViewsQuery } from '../../../../../../graphql/view.generated';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '../../../../view/utils';
import { useUserContext } from '../../../../../context/useUserContext';
import { DataHubViewType } from '../../../../../../types.generated';

const MatchingViewsLabel = () => {
    const userContext = useUserContext();
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;

    const StyledMatchingViewsLabel = styled.div`
        color: ${ANTD_GRAY[8]};
    `;

    /**
     * Fetch all personal/private views using listMyViews
     */
    const { data: personalViewsData } = useListMyViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
            viewType: DataHubViewType.Personal,
        },
        fetchPolicy: 'cache-first',
    });

    /**
     * Fetch all global/public views using listGlobalViews
     */
    const { data: globalViewsData } = useListGlobalViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
        },
        fetchPolicy: 'cache-first',
    });

    const onClear = () => {
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: undefined,
        });
    };

    const personalViews = personalViewsData?.listMyViews?.views || [];
    const globalViews = globalViewsData?.listGlobalViews?.views || [];

    /**
     * Check if selectedViewUrn exists in either the user's private or public views and if so use it
     */
    const selectedView = selectedViewUrn
        ? personalViews?.find((view) => view.urn === selectedViewUrn) ||
          globalViews?.find((view) => view.urn === selectedViewUrn)
        : undefined;

    return (
        <>
            {selectedView ? (
                <StyledMatchingViewsLabel>
                    Only showing entities in the
                    <Typography.Text strong> {selectedView?.name} </Typography.Text>
                    view.
                    <Button data-testid="view-select-clear" type="link" onClick={onClear}>
                        Clear view
                    </Button>
                </StyledMatchingViewsLabel>
            ) : (
                <div />
            )}
        </>
    );
};

export default MatchingViewsLabel;
