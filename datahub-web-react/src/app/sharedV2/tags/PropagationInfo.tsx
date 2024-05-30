import { Popover } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ThunderboltOutlined } from '@ant-design/icons';
import { usePropagationContextEntities, PropagationContext } from './usePropagationContextEntities';
import PropagationEntityLink from '../propagation/PropagationEntityLink';

const TooltipWrapper = styled.div`
    display: flex;
`;

const PropagateThunderbolt = styled(ThunderboltOutlined)`
    color: rgba(0, 143, 100, 0.95);
    font-weight: bold;
`;

interface Props {
    context?: string | null;
}

export default function PropagationInfo({ context }: Props) {
    const contextObj = context ? (JSON.parse(context) as PropagationContext) : null;
    const isPropagated = contextObj?.propagated;
    const { originEntity } = usePropagationContextEntities(contextObj);

    if (!isPropagated || !originEntity) return null;

    const tooltipContent = (
        <TooltipWrapper>
            Propagated from <PropagationEntityLink entity={originEntity} />
        </TooltipWrapper>
    );

    return (
        <Popover content={tooltipContent}>
            <PropagateThunderbolt />
        </Popover>
    );
}
