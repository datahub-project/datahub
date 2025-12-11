/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import Icon from '@ant-design/icons/lib/components/Icon';
import { Tooltip } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { BrowseV2EntityLinkClickEvent } from '@app/analytics';
import { useBrowseDisplayName, useIsBrowsePathSelected } from '@app/search/sidebar/BrowseContext';
import ExpandableNode from '@app/search/sidebar/ExpandableNode';
import useSidebarAnalytics from '@app/search/sidebar/useSidebarAnalytics';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, Maybe } from '@types';

import ExternalLink from '@images/link-out.svg?react';

const Linkicon = styled(Icon)<{ isSelected: boolean }>`
    && {
        color: ${(props) => props.theme.styles['primary-color']};
        ${(props) => !props.isSelected && 'display: none;'}
        ${ExpandableNode.SelectableHeader}:hover & {
            display: inherit;
        }
    }
`;

type Props = {
    entity?: Maybe<Entity>;
    targetNode: BrowseV2EntityLinkClickEvent['targetNode'];
};

// The tooltip needs some text to hold onto
const EmptySpace = styled.span`
    display: 'none';
    content: ' ';
`;

const EntityLink = ({ entity, targetNode }: Props) => {
    const registry = useEntityRegistry();
    const isBrowsePathSelected = useIsBrowsePathSelected();
    const displayName = useBrowseDisplayName();
    const { trackEntityLinkClickEvent } = useSidebarAnalytics();
    const entityUrl = entity ? registry.getEntityUrl(entity.type, entity.urn) : null;

    const onClickButton = () => {
        trackEntityLinkClickEvent(targetNode);
    };

    if (!entityUrl) return null;

    return (
        <Tooltip placement="top" title={`View ${displayName} profile`} mouseEnterDelay={1}>
            <Link to={entityUrl}>
                <ExpandableNode.StaticButton
                    icon={<Linkicon isSelected={isBrowsePathSelected} component={ExternalLink} />}
                    onClick={onClickButton}
                />
            </Link>
            <EmptySpace />
        </Tooltip>
    );
};

export default EntityLink;
