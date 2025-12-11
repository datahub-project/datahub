/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entity/view/utils';

import { useListGlobalViewsQuery, useListMyViewsQuery } from '@graphql/view.generated';
import { DataHubViewType } from '@types';

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
