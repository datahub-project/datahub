import { Tooltip } from '@components';
import { ChartLineUp } from '@phosphor-icons/react/dist/csr/ChartLineUp';
import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { pathMatchesInsensitiveToV2 } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { FieldPopularity } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldPopularity';
import { useBaseEntity } from '@src/app/entityV2/shared/EntityContext';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { UsageQueryResult } from '@types';

const IconsContainer = styled.div`
    display: flex;
    align-items: flex-end;
    gap: 6px;
    flex-direction: row;
`;

const IconWrapper = styled.div<{ hasStats: boolean; isFieldSelected: boolean }>`
    display: flex;
    svg {
        width: 16px;
        height: 16px;
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

    return useCallback(
        (fieldPath: string) => {
            const isFieldSelected = expandedDrawerFieldPath === fieldPath;

            const fieldProfile = latestProfile?.fieldProfiles?.find((profile) =>
                pathMatchesInsensitiveToV2(profile.fieldPath, fieldPath),
            );

            return (
                <IconsContainer>
                    <FieldPopularity isFieldSelected={isFieldSelected} usageStats={usageStats} fieldPath={fieldPath} />

                    {/* <Icon>
                        <LineageDisabledIcon height={20} width={20} />
                    </Icon> */}

                    <Tooltip
                        placement="top"
                        title={
                            !fieldProfile
                                ? t('usageStatsRenderer.noColumnStats')
                                : t('usageStatsRenderer.hasColumnStats')
                        }
                    >
                        <IconWrapper hasStats={!!fieldProfile} isFieldSelected={isFieldSelected}>
                            <ChartLineUp />
                        </IconWrapper>
                    </Tooltip>
                </IconsContainer>
            );
        },
        [expandedDrawerFieldPath, latestProfile, usageStats, t],
    );
}
