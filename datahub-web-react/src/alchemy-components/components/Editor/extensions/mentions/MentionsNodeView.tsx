/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import { NodeViewComponentProps } from '@remirror/react';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@src/app/entityV2/Entity';
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetEntityMentionNodeQuery } from '@src/graphql/search.generated';

const { Text } = Typography;

const InvalidEntityText = styled(Text)`
    display: inline-block;
    font-weight: 500;
    color: ${ANTD_GRAY[7]};
`;

const ValidEntityText = styled(Text)`
    display: inline-block;
    font-weight: 500;
    margin-left: 4px !important;
    word-break: break-all;
    color: ${(props) => props.theme.styles['primary-color']};
`;

// !important is needed to override inline styles
const Container = styled.span`
    & > .anticon {
        color: ${(props) => props.theme.styles['primary-color']} !important;
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
        return <ValidEntityText>{name}</ValidEntityText>;
    }

    if (!data || !data.entity) {
        return (
            <Tooltip title="Failed to find entity">
                <InvalidEntityText delete>{name}</InvalidEntityText>
            </Tooltip>
        );
    }

    const { entity } = data;
    const entityName = registry.getDisplayName(entity.type, entity);
    const entityType = registry.getIcon(entity.type, 14, IconStyleType.ACCENT);

    return (
        <HoverEntityTooltip entity={entity}>
            <Container>
                {entityType}
                <ValidEntityText>{entityName}</ValidEntityText>
            </Container>
        </HoverEntityTooltip>
    );
};
