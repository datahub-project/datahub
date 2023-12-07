import React from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { ViewOptionTooltipTitle } from './ViewOptionTooltipTitle';

const ViewName = styled.span`
    width: 200px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

type Props = {
    name: string;
    description?: string | null;
};

export const ViewOptionName = ({ name, description }: Props) => {
    return (
        <Tooltip placement="bottom" showArrow title={<ViewOptionTooltipTitle name={name} description={description} />}>
            <ViewName>{name}</ViewName>
        </Tooltip>
    );
};
