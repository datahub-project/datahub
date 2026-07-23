import { Button, Typography } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DataHubView } from '@types';

const StyledMatchingViewsLabel = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
`;

interface Props {
    view?: DataHubView;
    selectedViewUrn?: string;
    setSelectedViewUrn?: (viewUrn: string | undefined) => void;
}

const MatchingViewsLabel = ({ view, selectedViewUrn, setSelectedViewUrn }: Props) => {
    const { t } = useTranslation('entity.shared.components');
    if (selectedViewUrn === view?.urn) {
        return (
            <StyledMatchingViewsLabel>
                <Trans
                    t={t}
                    i18nKey="embeddedSearch.matchingViews"
                    components={{ strong: <Typography.Text strong /> }}
                    values={{ viewName: view?.name }}
                />
                <Button data-testid="view-select-clear" type="link" onClick={() => setSelectedViewUrn?.(undefined)}>
                    {t('embeddedSearch.clearView')}
                </Button>
            </StyledMatchingViewsLabel>
        );
    }

    return <div />;
};

export default MatchingViewsLabel;
