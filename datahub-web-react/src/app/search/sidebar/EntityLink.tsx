import { Tooltip } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import Icon from '@ant-design/icons/lib/components/Icon';
import { useBrowseDisplayName, useIsBrowsePathSelected } from './BrowseContext';
import ExpandableNode from './ExpandableNode';
import ExternalLink from '../../../images/link-out.svg?react';
import { useEntityRegistry } from '../../useEntityRegistry';
import { Entity, Maybe } from '../../../types.generated';
import useSidebarAnalytics from './useSidebarAnalytics';
import { BrowseV2EntityLinkClickEvent } from '../../analytics';

const Linkicon = styled(Icon)<{ $isSelected: boolean }>`
    && {
        color: ${(props) => props.theme.styles['primary-color']};
        ${(props) => !props.$isSelected && 'display: none;'}
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
                    icon={<Linkicon $isSelected={isBrowsePathSelected} component={ExternalLink} />}
                    onClick={onClickButton}
                />
            </Link>
            <EmptySpace />
        </Tooltip>
    );
};

export default EntityLink;
