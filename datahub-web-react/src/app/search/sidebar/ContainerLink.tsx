import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';
import Icon from '@ant-design/icons/lib/components/Icon';
import { useBrowseDisplayName, useIsBrowsePathSelected } from './BrowseContext';
import ExpandableNode from './ExpandableNode';
import { ReactComponent as ExternalLink } from '../../../images/link-out.svg';

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
    onClick: () => void;
};

// The tooltip needs some text to hold onto
const EmptySpace = styled.span`
    display: 'none';
    content: ' ';
`;

const ContainerLink = ({ onClick }: Props) => {
    const isBrowsePathSelected = useIsBrowsePathSelected();
    const displayName = useBrowseDisplayName();

    // todo - add a delay

    return (
        <Tooltip placement="top" title={`view ${displayName} profile`} mouseEnterDelay={1}>
            <ExpandableNode.StaticButton
                icon={<ContainerIcon isSelected={isBrowsePathSelected} component={ExternalLink} />}
                onClick={onClick}
            />
            <EmptySpace />
        </Tooltip>
    );
};

export default ContainerLink;
