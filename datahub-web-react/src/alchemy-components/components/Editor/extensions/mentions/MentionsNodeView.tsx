import { Tooltip } from '@components';
import { NodeViewComponentProps } from '@remirror/react';
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@src/app/entityV2/Entity';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetEntityMentionNodeQuery } from '@src/graphql/search.generated';

const InvalidEntityText = styled.span`
    font-weight: 500;
    color: ${(props) => props.theme.colors.textDisabled};
`;

const EntityName = styled.span`
    font-weight: 400;
    margin-left: 4px;
    word-break: break-all;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const Container = styled.span`
    display: inline-flex;
    align-items: baseline;
    vertical-align: baseline;
    color: ${(props) => props.theme.colors.icon};
    padding: 0 4px;
    border-radius: 4px;
    cursor: pointer;

    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

const IconWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 14px;
    height: 14px;
    flex-shrink: 0;
    position: relative;
    top: 2px;

    svg {
        display: block;
    }

    img {
        display: block;
        max-width: 100%;
        max-height: 100%;
    }
`;

export const MentionsNodeView = ({ node }: NodeViewComponentProps) => {
    const { urn, name } = node.attrs;

    const registry = useEntityRegistry();
    const { data, loading } = useGetEntityMentionNodeQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    if (loading) {
        return <EntityName>{name}</EntityName>;
    }

    if (!data || !data.entity) {
        return (
            <Tooltip title="Failed to find entity">
                <InvalidEntityText>{name}</InvalidEntityText>
            </Tooltip>
        );
    }

    const { entity } = data;
    const displayName = registry.getDisplayName(entity.type, entity);
    const icon = registry.getIcon(entity.type, 14, IconStyleType.ACCENT);

    return (
        <HoverEntityTooltip entity={entity}>
            <Container>
                <IconWrapper>{icon}</IconWrapper>
                <EntityName>{displayName}</EntityName>
            </Container>
        </HoverEntityTooltip>
    );
};
