import { geekblue } from '@ant-design/colors';
import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';
import QueryStatsOutlinedIcon from '@mui/icons-material/QueryStatsOutlined';
import { DatasetProfile, UsageQueryResult } from '../../../../../../../types.generated';
// import { ReactComponent as LineageDisabledIcon } from '../../../../../../../images/lineage-disabled-icon.svg';
import { GetDatasetQuery } from '../../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../../../../entity/shared/EntityContext';
import { REDESIGN_COLORS } from '../../../../constants';
import { FieldPopularity } from '../components/SchemaFieldDrawer/FieldPopularity';

export const UsageBar = styled.div<{ width: number }>`
    width: ${(props) => props.width}px;
    height: 4px;
    background-color: ${geekblue[3]};
    border-radius: 2px;
`;

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
            if (props.isFieldSelected) {
                return `${REDESIGN_COLORS.WHITE}`;
            }
            return props.hasStats ? '#533fd1' : '#C6C0E0';
        }};
        opacity: ${(props) => (props.isFieldSelected && !props.hasStats ? '0.5' : '')};
    }
`;

export default function useUsageStatsRenderer(
    usageStats?: UsageQueryResult | null,
    expandedDrawerFieldPath?: string | null,
) {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const hasDatasetProfiles = baseEntity?.dataset?.datasetProfiles !== undefined;
    const datasetProfiles =
        (hasDatasetProfiles && (baseEntity?.dataset?.datasetProfiles as Array<DatasetProfile>)) || undefined;

    const latestProfile = datasetProfiles && datasetProfiles[0];

    const usageStatsRenderer = (fieldPath: string) => {
        const isFieldSelected = expandedDrawerFieldPath === fieldPath;

        const fieldProfile = latestProfile?.fieldProfiles?.find((profile) => profile.fieldPath === fieldPath);

        return (
            <IconsContainer>
                <FieldPopularity isFieldSelected={isFieldSelected} usageStats={usageStats} fieldPath={fieldPath} />

                {/* <Icon>
                        <LineageDisabledIcon height={20} width={20} />
                    </Icon> */}

                <Tooltip placement="top" title={!fieldProfile ? 'No column statistics' : 'Has column statistics'}>
                    <IconWrapper hasStats={!!fieldProfile} isFieldSelected={isFieldSelected}>
                        <QueryStatsOutlinedIcon />
                    </IconWrapper>
                </Tooltip>
            </IconsContainer>
        );
    };
    return usageStatsRenderer;
}
