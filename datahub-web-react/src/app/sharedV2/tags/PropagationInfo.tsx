import { Popover } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ThunderboltOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useEntityRegistryV2 } from '../../useEntityRegistry';
import { usePropagationContextEntities, PropagationContext } from './usePropagationContextEntities';

const StyledLink = styled(Link)`
    font-weight: 600;
`;

const PropagateThunderbolt = styled(ThunderboltOutlined)`
    color: rgba(0, 143, 100, 0.95);
    font-weight: bold;
`;

interface Props {
    context?: string | null;
}

export default function PropagationInfo({ context }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const contextObj = context ? (JSON.parse(context) as PropagationContext) : null;
    const isPropagated = contextObj?.propagated;
    const { originEntity, actorEntity } = usePropagationContextEntities(contextObj);

    let tooltipContent = <>This metadata was propagated</>;
    if (originEntity && !actorEntity) {
        tooltipContent = (
            <>
                This metadata was propagated from{' '}
                <StyledLink to={entityRegistry.getEntityUrl(originEntity.type, originEntity.urn)}>
                    {entityRegistry.getDisplayName(originEntity.type, originEntity)}
                </StyledLink>{' '}
            </>
        );
    } else if (actorEntity && !originEntity) {
        tooltipContent = (
            <>
                This metadata was propagated by{' '}
                <StyledLink to={entityRegistry.getEntityUrl(actorEntity.type, actorEntity.urn)}>
                    {entityRegistry.getDisplayName(actorEntity.type, actorEntity)}
                </StyledLink>
            </>
        );
    } else if (originEntity && actorEntity) {
        tooltipContent = (
            <>
                This metadata was propagated from&nbsp;
                <StyledLink
                    to={`${window.location.origin}${entityRegistry.getEntityUrl(originEntity.type, originEntity.urn)}`}
                    target="_blank"
                    rel="noopener noreferrer"
                >
                    {entityRegistry.getDisplayName(originEntity.type, originEntity)}
                </StyledLink>{' '}
                by{' '}
                <StyledLink
                    to={entityRegistry.getEntityUrl(actorEntity.type, actorEntity.urn)}
                    target="_blank"
                    rel="noopener noreferrer"
                >
                    {entityRegistry.getDisplayName(actorEntity.type, actorEntity)}
                </StyledLink>
            </>
        );
    }

    if (!isPropagated) return null;

    return (
        <Popover content={tooltipContent}>
            <PropagateThunderbolt />
        </Popover>
    );
}
