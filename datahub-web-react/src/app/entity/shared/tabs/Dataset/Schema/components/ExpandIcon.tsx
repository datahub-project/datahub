import React from 'react';
import { DownOutlined, RightOutlined } from '@ant-design/icons';
import { RenderExpandIconProps } from 'rc-table/lib/interface';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../constants';

const Prefix = styled.div<{ padding: number }>`
    padding-left: ${(props) => props.padding}px;
    position: absolute;
    min-height: 100%;
    border-right: 2px solid ${ANTD_GRAY[4]};
    border-top: 1px solid white;
    border-bottom: 1px solid white;
    margin-bottom: -1px;
    top: -1px;
`;

const Padding = styled.span<{ padding: number }>`
    margin-left: ${(props) => props.padding}px;
`;

const Down = styled(DownOutlined)`
    color: ${ANTD_GRAY[7]};
    padding-right: 5px;
    padding-top: 21px;
    vertical-align: top;
`;

const Right = styled(RightOutlined)`
    color: ${ANTD_GRAY[7]};
    padding-right: 5px;
    padding-top: 21px;
    vertical-align: top;
`;

const DEPTH_PADDING = 15;

export default function ExpandIcon({ expanded, onExpand, expandable, record }: RenderExpandIconProps<any>) {
    return (
        <>
            {Array.from({ length: record.depth }, (_, k) => (
                <Prefix padding={5 + DEPTH_PADDING * (k + 1)} />
            ))}
            <Padding padding={DEPTH_PADDING * (record.depth + 1)} />
            {expandable &&
                record.children !== undefined &&
                (expanded ? (
                    <Down onClick={(e) => onExpand(record, e)} width={12} height={12} />
                ) : (
                    <Right onClick={(e) => onExpand(record, e)} width={12} height={12} />
                ))}
        </>
    );
}
