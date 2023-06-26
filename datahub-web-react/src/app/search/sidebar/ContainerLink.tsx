import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';
import Icon from '@ant-design/icons/lib/components/Icon';
import { useHistory } from 'react-router';
import { useBrowseDisplayName, useIsBrowsePathSelected } from './BrowseContext';
import ExpandableNode from './ExpandableNode';
import { ReactComponent as ExternalLink } from '../../../images/link-out.svg';
import { useEntityRegistry } from '../../useEntityRegistry';
import { Entity } from '../../../types.generated';
import useSidebarAnalytics from './useSidebarAnalytics';

// todo - this hover rule feels a little weird
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
    entity: Entity;
};

// The tooltip needs some text to hold onto
const EmptySpace = styled.span`
    display: 'none';
    content: ' ';
`;

const ContainerLink = ({ entity }: Props) => {
    const registry = useEntityRegistry();
    const history = useHistory();
    const isBrowsePathSelected = useIsBrowsePathSelected();
    const displayName = useBrowseDisplayName();
    const { trackClickContainerLinkEvent } = useSidebarAnalytics();

    const onClickButton = () => {
        trackClickContainerLinkEvent();
        const containerUrl = registry.getEntityUrl(entity.type, entity.urn);
        history.push(containerUrl);
    };

    return (
        <Tooltip placement="top" title={`view ${displayName} profile`} mouseEnterDelay={1}>
            <ExpandableNode.StaticButton
                icon={<ContainerIcon isSelected={isBrowsePathSelected} component={ExternalLink} />}
                onClick={onClickButton}
            />
            <EmptySpace />
        </Tooltip>
    );
};

export default ContainerLink;
