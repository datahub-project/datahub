import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useBaseEntity, useRouteToTab } from '@app/entity/shared/EntityContext';
import { InfoItem } from '@app/entity/shared/components/styled/InfoItem';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';

import { GetDatasetQuery } from '@graphql/dataset.generated';

const HeaderInfoBody = styled(Typography.Text)`
    font-size: 16px;
    color: ${ANTD_GRAY[9]};
`;

const HeaderContainer = styled.div`
    justify-content: space-between;
    display: flex;
`;

const StatsButton = styled(Button)`
    margin-top: -4px;
`;

const InfoRow = styled.div`
    padding-top: 12px;
    padding-bottom: 12px;
`;

const INFO_ITEM_WIDTH_PX = '150px';

export const SidebarViewDefinitionSection = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const materialized = baseEntity?.dataset?.viewProperties?.materialized;
    const language = baseEntity?.dataset?.viewProperties?.language || 'UNKNOWN';

    const routeToTab = useRouteToTab();

    return (
        <div>
            <HeaderContainer>
                <SidebarHeader title="View Definition" />
                <StatsButton onClick={() => routeToTab({ tabName: 'View Definition' })} type="link">
                    See View Definition &gt;
                </StatsButton>
            </HeaderContainer>
            <InfoRow>
                <InfoItem title="Materialized" width={INFO_ITEM_WIDTH_PX}>
                    <HeaderInfoBody>{materialized ? 'True' : 'False'}</HeaderInfoBody>
                </InfoItem>
                <InfoItem title="Language" width={INFO_ITEM_WIDTH_PX}>
                    <HeaderInfoBody>{language.toUpperCase()}</HeaderInfoBody>
                </InfoItem>
            </InfoRow>
        </div>
    );
};
