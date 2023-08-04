import React from 'react';
import { Button, Typography } from 'antd';
import { useListGlobalViewsQuery, useListMyViewsQuery } from '../../../../../../graphql/view.generated';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '../../../../view/utils';
import { useUserContext } from '../../../../../context/useUserContext';

const MatchingViewsLabel = () => {
    const userContext = useUserContext();
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;

    /**
     * Fetch all private views using listMyViews
     */
    const { data: privateViewsData } = useListMyViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
        },
        fetchPolicy: 'cache-first',
    });

    /**
     * Fetch all global/public views using listGlobalViews
     */
    const { data: publicViewsData } = useListGlobalViewsQuery({
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

    const privateViews = privateViewsData?.listMyViews?.views || [];
    const publicViews = publicViewsData?.listGlobalViews?.views || [];

    /**
     * Check if selectedViewUrn exists in either the private or public views and if so use it
     */
    const selectedView = selectedViewUrn
        ? privateViews?.find((view) => view.urn === selectedViewUrn) ||
          publicViews?.find((view) => view.urn === selectedViewUrn)
        : undefined;

    if (selectedView) {
        return (
            <div>
                Only showing entities in the
                <Typography.Text strong> {selectedView?.name} </Typography.Text>
                view.
                <Button data-testid="view-select-clear" type="link" onClick={onClear}>
                    Clear view
                </Button>
            </div>
        );
    }

    return null;
};

export default MatchingViewsLabel;
