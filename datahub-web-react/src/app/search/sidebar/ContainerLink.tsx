import { Tooltip } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import Icon from '@ant-design/icons/lib/components/Icon';
import { useBrowseDisplayName, useIsBrowsePathSelected } from './BrowseContext';
import ExpandableNode from './ExpandableNode';
import { ReactComponent as ExternalLink } from '../../../images/link-out.svg';
import { useEntityRegistry } from '../../useEntityRegistry';
import { Entity, Maybe } from '../../../types.generated';
import useSidebarAnalytics from './useSidebarAnalytics';

const ContainerIcon = styled(Icon)<{ isSelected: boolean }>`
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
};

// The tooltip needs some text to hold onto
const EmptySpace = styled.span`
    display: 'none';
    content: ' ';
`;

const ContainerLink = ({ entity }: Props) => {
    const registry = useEntityRegistry();
    const isBrowsePathSelected = useIsBrowsePathSelected();
    const displayName = useBrowseDisplayName();
    const { trackClickContainerLinkEvent } = useSidebarAnalytics();
    const containerUrl = entity ? registry.getEntityUrl(entity.type, entity.urn) : null;

    const onClickButton = () => {
        trackClickContainerLinkEvent();
    };

    if (!containerUrl) return null;

    return (
        <Tooltip placement="top" title={`View ${displayName} profile`} mouseEnterDelay={1}>
            <Link to={containerUrl}>
                <ExpandableNode.StaticButton
                    icon={<ContainerIcon isSelected={isBrowsePathSelected} component={ExternalLink} />}
                    onClick={onClickButton}
                />
            </Link>
            <EmptySpace />
        </Tooltip>
    );
};

export default ContainerLink;
