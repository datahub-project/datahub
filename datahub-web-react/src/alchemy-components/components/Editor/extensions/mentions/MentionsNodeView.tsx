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
    text-decoration: line-through;
    color: ${(props) => props.theme.colors.textDisabled};
`;

const EntityName = styled.span`
    font-weight: 500;
    margin-left: 4px;
    word-break: break-all;
    color: ${(props) => props.theme.colors.text};
`;

const Container = styled.span`
    display: inline-flex;
    align-items: center;
    vertical-align: middle;
    color: ${(props) => props.theme.colors.icon};
`;

const IconWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 16px;
    height: 16px;
    flex-shrink: 0;

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
