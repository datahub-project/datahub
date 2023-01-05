import React from 'react';
import { Tooltip } from 'antd';
import dayjs from 'dayjs';
import LocalizedFormat from 'dayjs/plugin/localizedFormat';
import { Group } from '@vx/group';
import { curveBasis } from '@vx/curve';
import { LinePath } from '@vx/shape';
import { VizEdge } from './types';
import { ANTD_GRAY } from '../entity/shared/constants';

dayjs.extend(LocalizedFormat);

type Props = {
    edge: VizEdge;
    key: string;
    isHighlighted: boolean;
};

export default function LineageEntityEdge({ edge, key, isHighlighted }: Props) {
    const createdOnTimestamp = edge?.createdOn;
    const updatedOnTimestamp =
        createdOnTimestamp && edge?.updatedOn && edge?.updatedOn > createdOnTimestamp
            ? edge?.updatedOn
            : createdOnTimestamp;
    const createdOn: string = createdOnTimestamp ? dayjs(createdOnTimestamp).format('ll') : 'unknown';
    const updatedOn: string = updatedOnTimestamp ? dayjs(updatedOnTimestamp).format('ll') : 'unknown';
    const isManual = edge?.isManual;

    return (
        <>
            <Tooltip
                arrowPointAtCenter
                title={
                    <>
                        Created: {createdOn}
                        <br />
                        Last Observed: {updatedOn}
                    </>
                }
            >
                <Group key={key}>
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
                        stroke={isHighlighted ? '#1890FF' : ANTD_GRAY[6]}
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
