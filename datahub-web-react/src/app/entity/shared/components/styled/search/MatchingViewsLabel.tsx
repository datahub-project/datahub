import React from 'react';
import { Button, Typography } from 'antd';
import { useListMyViewsQuery } from '../../../../../../graphql/view.generated';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '../../../../view/utils';
import { useUserContext } from '../../../../../context/useUserContext';

const MatchingViewsLabel = () => {
    const userContext = useUserContext();
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;

    const { data } = useListMyViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
        },
        fetchPolicy: 'cache-first',
    });

    const selectedView = selectedViewUrn
        ? data?.listMyViews?.views.find((view) => view.urn === selectedViewUrn)
        : undefined;

    const onClear = () => {
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: undefined,
        });
    };

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

    return <Typography.Text>No view applied.</Typography.Text>;
};

export default MatchingViewsLabel;
