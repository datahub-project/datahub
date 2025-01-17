import React from 'react';
import styled from 'styled-components';
import { Button, Typography } from 'antd';
import { GetDatasetQuery } from '../../../../../../../../graphql/dataset.generated';
import { InfoItem } from '../../../../../components/styled/InfoItem';
import { ANTD_GRAY } from '../../../../../constants';
import { useBaseEntity, useRouteToTab } from '../../../../../../../entity/shared/EntityContext';
import { SidebarSection } from '../../SidebarSection';

const HeaderInfoBody = styled(Typography.Text)`
    font-size: 16px;
    color: ${ANTD_GRAY[9]};
`;

const StatsButton = styled(Button)`
    && {
        margin: 0px;
        padding: 0px;
    }
`;

const InfoRow = styled.div`
    padding-top: 8px;
    padding-bottom: 8px;
`;

const INFO_ITEM_WIDTH_PX = '150px';

export const SidebarViewDefinitionSection = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const materialized = baseEntity?.dataset?.viewProperties?.materialized;
    const language = baseEntity?.dataset?.viewProperties?.language || 'UNKNOWN';

    const routeToTab = useRouteToTab();

    return (
        <SidebarSection
            title="Definition"
            content={
                <>
                    <InfoRow>
                        <InfoItem title="Materialized" width={INFO_ITEM_WIDTH_PX}>
                            <HeaderInfoBody>{materialized ? 'True' : 'False'}</HeaderInfoBody>
                        </InfoItem>
                        <InfoItem title="Language" width={INFO_ITEM_WIDTH_PX}>
                            <HeaderInfoBody>{language.toUpperCase()}</HeaderInfoBody>
                        </InfoItem>
                    </InfoRow>
                    <StatsButton onClick={() => routeToTab({ tabName: 'View Definition' })} type="link">
                        View full definition &gt;
                    </StatsButton>
                </>
            }
        />
    );
};
