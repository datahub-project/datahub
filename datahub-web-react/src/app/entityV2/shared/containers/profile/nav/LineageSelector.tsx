import React from 'react';
import styled from 'styled-components/macro';
import { useHistory, useLocation } from 'react-router-dom';
import { Badge } from 'antd';
import { InfoCircleOutlined, PartitionOutlined } from '@ant-design/icons';
import { grey, blue } from '@ant-design/colors';
import { EntityType } from '../../../../../../types.generated';
import { navigateToLineageUrl } from '../../../../../lineage/utils/navigateToLineageUrl';
import { ANTD_GRAY, ENTITY_TYPES_WITH_MANUAL_LINEAGE } from '../../../constants';
import useIsLineageMode from '../../../../../lineage/utils/useIsLineageMode';
import { useGetLineageTimeParams } from '../../../../../lineage/utils/useGetLineageTimeParams';
import { useIsSeparateSiblingsMode } from '../../../siblingUtils';
import { useGetLineageCountsQuery } from '../../../../../../graphql/lineage.generated';

const LineageIconGroup = styled.div`
    width: 180px;
    display: flex;
    justify-content: space-between;
    margin-right: 8px;
`;

const LineageIcon = styled(PartitionOutlined)`
    font-size: 20px;
    vertical-align: middle;
    padding-right: 6px;
`;

const DetailIcon = styled(InfoCircleOutlined)`
    font-size: 20px;
    vertical-align: middle;
    padding-right: 6px;
`;

const IconGroup = styled.div<{ isSelected: boolean; disabled?: boolean }>`
    font-size: 14px;
    color: ${(props) => {
        if (props.disabled) {
            return grey[2];
        }
        return !props.isSelected ? 'black' : props.theme.styles['primary-color'] || blue[4];
    }};
    &:hover {
        color: ${(props) => (props.disabled ? grey[2] : props.theme.styles['primary-color'] || blue[4])};
        cursor: ${(props) => (props.disabled ? 'not-allowed' : 'pointer')};
    }
`;

const LineageNavContainer = styled.div`
    display: inline-flex;
    line-height: 24px;
    align-items: center;
`;

const LineageSummary = styled.div`
    margin-left: 16px;
`;

const LineageBadge = styled(Badge)`
    &&& .ant-badge-count {
        background-color: ${ANTD_GRAY[1]};
        color: ${ANTD_GRAY[9]};
        border: 1px solid ${ANTD_GRAY[5]};
        font-size: 12px;
        font-weight: 600;
        height: 22px;
    }
`;

type Props = {
    urn: string;
    type: EntityType;
};

/**
 * Responsible for rendering a clickable browse path view.
 */
export const LineageSelector = ({ urn, type }: Props): JSX.Element => {
    const history = useHistory();
    const location = useLocation();
    const isLineageMode = useIsLineageMode();
    const isHideSiblingsMode = useIsSeparateSiblingsMode();
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    // Fetch the lineage counts for the entity.
    const { data: lineageData, loading: lineageLoading } = useGetLineageCountsQuery({
        variables: {
            urn,
            separateSiblings: isHideSiblingsMode,
            startTimeMillis,
            endTimeMillis,
        },
    });

    const upstreamTotal = (lineageData?.entity as any)?.upstream?.total || 0;
    const upstreamFiltered = (lineageData?.entity as any)?.upstream?.filtered || 0;
    const upstreamCount = upstreamTotal - upstreamFiltered;

    const downstreamTotal = (lineageData?.entity as any)?.downstream?.total || 0;
    const downstreamFiltered = (lineageData?.entity as any)?.downstream?.filtered || 0;
    const downstreamCount = downstreamTotal - downstreamFiltered;

    const hasLineage = upstreamCount > 0 || downstreamCount > 0;
    const canNavigateToLineage = hasLineage || ENTITY_TYPES_WITH_MANUAL_LINEAGE.has(type);

    const upstreamText = upstreamCount === 100 ? '100+' : upstreamCount;
    const downstreamText = downstreamCount === 100 ? '100+' : downstreamCount;

    return (
        <LineageNavContainer>
            <LineageIconGroup>
                <IconGroup
                    disabled={!canNavigateToLineage}
                    isSelected={!isLineageMode}
                    onClick={() => {
                        if (canNavigateToLineage) {
                            navigateToLineageUrl({
                                location,
                                history,
                                isLineageMode: false,
                                startTimeMillis,
                                endTimeMillis,
                            });
                        }
                    }}
                >
                    <DetailIcon />
                    Details
                </IconGroup>
                <IconGroup
                    disabled={!canNavigateToLineage}
                    isSelected={isLineageMode}
                    onClick={() => {
                        if (canNavigateToLineage) {
                            navigateToLineageUrl({
                                location,
                                history,
                                isLineageMode: true,
                                startTimeMillis,
                                endTimeMillis,
                            });
                        }
                    }}
                >
                    <LineageIcon />
                    Lineage
                </IconGroup>
            </LineageIconGroup>
            <LineageSummary>
                <LineageBadge
                    count={`${lineageLoading ? '-' : upstreamText} upstream, ${
                        lineageLoading ? '-' : downstreamText
                    } downstream`}
                />
            </LineageSummary>
        </LineageNavContainer>
    );
};
