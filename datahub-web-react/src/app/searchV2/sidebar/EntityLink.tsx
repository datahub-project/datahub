import { Tooltip } from '@components';
import { ArrowSquareOut } from '@phosphor-icons/react/dist/csr/ArrowSquareOut';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { BrowseV2EntityLinkClickEvent } from '@app/analytics';
import { useBrowseDisplayName } from '@app/searchV2/sidebar/BrowseContext';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { TREE_ROW_HOVER_ACTIONS_CLASS } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/treeRow.styles';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, Maybe } from '@types';

const LinkWrap = styled.div`
    display: flex;
    align-items: center;
`;

const ProfileLink = styled(Link)`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 2px;
    color: ${(props) => props.theme.colors.icon};

    &:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

type Props = {
    entity?: Maybe<Entity>;
    targetNode: BrowseV2EntityLinkClickEvent['targetNode'];
};

const EntityLink = ({ entity, targetNode }: Props) => {
    const { t } = useTranslation('search');
    const registry = useEntityRegistry();
    const displayName = useBrowseDisplayName();
    const { trackEntityLinkClickEvent } = useSidebarAnalytics();
    const entityUrl = entity ? registry.getEntityUrl(entity.type, entity.urn) : null;

    const onClickLink = (event: React.MouseEvent) => {
        event.stopPropagation();
        trackEntityLinkClickEvent(targetNode);
    };

    if (!entityUrl) return null;

    const label = t('sidebar.viewEntityProfile', { name: displayName });

    return (
        <LinkWrap className={TREE_ROW_HOVER_ACTIONS_CLASS} onClick={(event) => event.stopPropagation()}>
            <Tooltip placement="top" title={label} mouseEnterDelay={1} showArrow={false}>
                <ProfileLink to={entityUrl} onClick={onClickLink} aria-label={label}>
                    <ArrowSquareOut size={16} />
                </ProfileLink>
            </Tooltip>
        </LinkWrap>
    );
};

export default EntityLink;
