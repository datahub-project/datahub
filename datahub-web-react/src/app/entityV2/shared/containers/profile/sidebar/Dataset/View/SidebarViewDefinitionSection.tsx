import { Button } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useBaseEntity, useRouteToTab } from '@app/entity/shared/EntityContext';
import { InfoItem } from '@app/entityV2/shared/components/styled/InfoItem';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

import { GetDatasetQuery } from '@graphql/dataset.generated';

const HeaderInfoBody = styled.span`
    font-size: 16px;
    color: ${(props) => props.theme.colors.text};
`;

const InfoRow = styled.div`
    padding-top: 8px;
    padding-bottom: 8px;
`;

const INFO_ITEM_WIDTH_PX = '150px';
// eslint-disable-next-line i18next/no-literal-string -- route tab name identifier, not UI text
const VIEW_DEFINITION_TAB = 'View Definition';

export const SidebarViewDefinitionSection = () => {
    const { t } = useTranslation('entity.shared.containers');
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const materialized = baseEntity?.dataset?.viewProperties?.materialized;
    const language = baseEntity?.dataset?.viewProperties?.language || 'UNKNOWN';

    const routeToTab = useRouteToTab();

    return (
        <SidebarSection
            title={t('sidebar.viewDefinition.sectionTitle')}
            content={
                <>
                    <InfoRow>
                        <InfoItem title={t('sidebar.viewDefinition.materializedLabel')} width={INFO_ITEM_WIDTH_PX}>
                            <HeaderInfoBody>
                                {materialized
                                    ? t('sidebar.viewDefinition.materializedTrue')
                                    : t('sidebar.viewDefinition.materializedFalse')}
                            </HeaderInfoBody>
                        </InfoItem>
                        <InfoItem title={t('sidebar.viewDefinition.languageLabel')} width={INFO_ITEM_WIDTH_PX}>
                            <HeaderInfoBody>{language.toUpperCase()}</HeaderInfoBody>
                        </InfoItem>
                    </InfoRow>
                    <Button variant="link" color="violet" onClick={() => routeToTab({ tabName: VIEW_DEFINITION_TAB })}>
                        {t('sidebar.viewDefinition.viewFullDefinitionLink')}
                    </Button>
                </>
            }
        />
    );
};
