import { ClockCircleOutlined, EyeOutlined } from '@ant-design/icons';
import { curveBasis } from '@visx/curve';
import { Group } from '@visx/group';
import { LinePath } from '@visx/shape';
import { Tooltip } from 'antd';
import dayjs from '@utils/dayjs';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { VizEdge } from '@app/lineage/types';

const EdgeTimestamp = styled.div``;

const StyledClockCircleOutlined = styled(ClockCircleOutlined)`
    margin-right: 4px;
    font-size: 14px;
`;

const StyledEyeOutlined = styled(EyeOutlined)`
    margin-right: 4px;
    font-size: 14px;
`;

type Props = {
    edge: VizEdge;
    edgeKey: string;
    isHighlighted: boolean;
};

export default function LineageEntityEdge({ edge, edgeKey, isHighlighted }: Props) {
    const theme = useTheme();
    const createdOnTimestamp = edge?.createdOn;
    const updatedOnTimestamp = edge?.updatedOn;
    const createdOn = createdOnTimestamp ? dayjs(createdOnTimestamp).format('ll') : undefined;
    const updatedOn = updatedOnTimestamp ? dayjs(updatedOnTimestamp).format('ll') : undefined;
    const hasTimestamps = createdOn || updatedOn;
    const isManual = edge?.isManual;

    return (
        <>
            <Tooltip
                title={
                    (hasTimestamps && (
                        <>
                            {createdOn && (
                                <EdgeTimestamp>
                                    <StyledClockCircleOutlined /> Created {isManual && 'manually '}on {createdOn}
                                </EdgeTimestamp>
                            )}
                            {updatedOn && !isManual && (
                                <EdgeTimestamp>
                                    <StyledEyeOutlined /> Last observed on {updatedOn}
                                </EdgeTimestamp>
                            )}
                        </>
                    )) ||
                    undefined
                }
            >
                <Group key={edgeKey}>
                    <LinePath
                        // we rotated the svg 90 degrees so we need to switch x & y for the last mile
                        x={(d) => {
                            // setX(d.y);
                            return d.y;
                        }}
                        y={(d) => {
                            // setY(d.x);
                            return d.x;
                        }}
                        curve={curveBasis}
                        data={edge.curve}
                        stroke={isHighlighted ? theme.colors.hyperlinks : theme.colors.border}
                        strokeWidth="1"
                        markerEnd={`url(#triangle-downstream${isHighlighted ? '-highlighted' : ''})`}
                        markerStart={`url(#triangle-upstream${isHighlighted ? '-highlighted' : ''})`}
                        data-testid={`edge-${edge.source.data.urn}-${edge.target.data.urn}-${edge.target.direction}`}
                        strokeDasharray={isManual ? '5, 5' : 'none'}
                    />
                </Group>
            </Tooltip>
        </>
    );
}
