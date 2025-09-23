import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

import { DataHubView } from '@types';

const StyledMatchingViewsLabel = styled.div`
    color: ${ANTD_GRAY[8]};
`;

interface Props {
    view?: DataHubView;
    selectedViewUrn?: string;
    setSelectedViewUrn?: (viewUrn: string | undefined) => void;
}

const MatchingViewsLabel = ({ view, selectedViewUrn, setSelectedViewUrn }: Props) => {
    if (selectedViewUrn === view?.urn) {
        return (
            <StyledMatchingViewsLabel>
                Only showing entities in the
                <Typography.Text strong> {view?.name} </Typography.Text>
                view.
                <Button data-testid="view-select-clear" type="link" onClick={() => setSelectedViewUrn?.(undefined)}>
                    Clear view
                </Button>
            </StyledMatchingViewsLabel>
        );
    }

    return <div />;
};

export default MatchingViewsLabel;
