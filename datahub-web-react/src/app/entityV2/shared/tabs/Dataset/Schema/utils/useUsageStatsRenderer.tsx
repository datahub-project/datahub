import { Tooltip } from '@components';
import QueryStatsOutlinedIcon from '@mui/icons-material/QueryStatsOutlined';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { FieldPopularity } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldPopularity';
import { useBaseEntity } from '@src/app/entity/shared/EntityContext';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { UsageQueryResult } from '@types';

const IconsContainer = styled.div`
    display: flex;
    gap: 3px;
    flex-direction: row;
`;

const IconWrapper = styled.div<{ hasStats: boolean; isFieldSelected: boolean }>`
    display: flex;
    svg {
        width: 18px;
        height: 18px;
        color: ${(props) => {
            return props.hasStats ? props.theme.colors.iconBrand : props.theme.colors.chartsBrandBase;
        }};
        opacity: ${(props) => (props.isFieldSelected && !props.hasStats ? '0.5' : '')};
    }
`;

export default function useUsageStatsRenderer(
    usageStats?: UsageQueryResult | null,
    expandedDrawerFieldPath?: string | null,
) {
    const { t } = useTranslation('entity.profile.schema');
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const latestFullTableProfile = baseEntity?.dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = baseEntity?.dataset?.latestPartitionProfile?.[0];

    const latestProfile = latestFullTableProfile || latestPartitionProfile;

    const usageStatsRenderer = (fieldPath: string) => {
        const isFieldSelected = expandedDrawerFieldPath === fieldPath;

        const fieldProfile = latestProfile?.fieldProfiles?.find((profile) => profile.fieldPath === fieldPath);

        return (
            <IconsContainer>
                <FieldPopularity isFieldSelected={isFieldSelected} usageStats={usageStats} fieldPath={fieldPath} />

                {/* <Icon>
                        <LineageDisabledIcon height={20} width={20} />
                    </Icon> */}

                <Tooltip
                    placement="top"
                    title={
                        !fieldProfile ? t('usageStatsRenderer.noColumnStats') : t('usageStatsRenderer.hasColumnStats')
                    }
                >
                    <IconWrapper hasStats={!!fieldProfile} isFieldSelected={isFieldSelected}>
                        <QueryStatsOutlinedIcon />
                    </IconWrapper>
                </Tooltip>
            </IconsContainer>
        );
    };
    return usageStatsRenderer;
}
