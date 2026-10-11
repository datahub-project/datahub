import { Button, Typography } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entity/view/utils';

import { useListGlobalViewsQuery, useListMyViewsQuery } from '@graphql/view.generated';
import { DataHubViewType } from '@types';

const StyledMatchingViewsLabel = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
`;

const MatchingViewsLabel = () => {
    const { t } = useTranslation('entityV1.shared.components');
    const userContext = useUserContext();
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;

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
                    <Trans
                        t={t}
                        i18nKey="matchingViews.label"
                        values={{ viewName: selectedView?.name }}
                        components={{ strong: <Typography.Text strong /> }}
                    />
                    <Button data-testid="view-select-clear" type="link" onClick={onClear}>
                        {t('matchingViews.clear')}
                    </Button>
                </StyledMatchingViewsLabel>
            ) : (
                <div />
            )}
        </>
    );
};

export default MatchingViewsLabel;
