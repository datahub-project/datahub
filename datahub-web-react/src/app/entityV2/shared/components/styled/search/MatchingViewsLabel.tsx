import { Button } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DataHubView } from '@types';

const StyledMatchingViewsLabel = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
`;

const ViewName = styled.strong`
    font-weight: 600;
`;

// alchemy link buttons have no padding; keep the gap antd's link button padding gave.
const ClearButton = styled(Button)`
    margin-left: 15px;
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
                    components={{ strong: <ViewName /> }}
                    values={{ viewName: view?.name }}
                />
                <ClearButton
                    data-testid="view-select-clear"
                    variant="link"
                    onClick={() => setSelectedViewUrn?.(undefined)}
                >
                    {t('embeddedSearch.clearView')}
                </ClearButton>
            </StyledMatchingViewsLabel>
        );
    }

    return <div />;
};

export default MatchingViewsLabel;
