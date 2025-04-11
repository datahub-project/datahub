import React from 'react';
import { Typography } from 'antd';
import { Tooltip } from '@components';
import styled from 'styled-components';

import { NodeViewComponentProps } from '@remirror/react';
import { HoverEntityTooltip } from '../../../../../../../../recommendations/renderer/component/HoverEntityTooltip';
import { useGetEntityMentionNodeQuery } from '../../../../../../../../../graphql/search.generated';
import { useEntityRegistry } from '../../../../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../../../../Entity';
import { ANTD_GRAY } from '../../../../../../constants';

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
