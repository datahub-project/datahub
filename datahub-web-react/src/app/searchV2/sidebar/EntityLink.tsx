import { Tooltip } from '@components';
import { ArrowSquareOut } from '@phosphor-icons/react/dist/csr/ArrowSquareOut';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { BrowseV2EntityLinkClickEvent } from '@app/analytics';
import { useBrowseDisplayName, useIsBrowsePathSelected } from '@app/searchV2/sidebar/BrowseContext';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import {
    TREE_ROW_HOVER_ACTIONS_CLASS,
    TREE_ROW_HOVER_ACTIONS_PINNED_CLASS,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/treeRow.styles';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, Maybe } from '@types';

const LinkButton = styled(Link)`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    color: ${(props) => props.theme.colors.iconBrand};
`;

type Props = {
    entity?: Maybe<Entity>;
    targetNode: BrowseV2EntityLinkClickEvent['targetNode'];
};

/**
 * "View profile" link on a browse row: revealed on row hover (shared tree-row hover actions),
 * and kept visible while its row is the selected browse path.
 */
const EntityLink = ({ entity, targetNode }: Props) => {
    const { t } = useTranslation('search');
    const registry = useEntityRegistry();
    const isBrowsePathSelected = useIsBrowsePathSelected();
    const displayName = useBrowseDisplayName();
    const { trackEntityLinkClickEvent } = useSidebarAnalytics();
    const entityUrl = entity ? registry.getEntityUrl(entity.type, entity.urn) : null;

    if (!entityUrl) return null;

    const className = isBrowsePathSelected
        ? `${TREE_ROW_HOVER_ACTIONS_CLASS} ${TREE_ROW_HOVER_ACTIONS_PINNED_CLASS}`
        : TREE_ROW_HOVER_ACTIONS_CLASS;

    return (
        <span className={className}>
            <Tooltip placement="top" title={t('sidebar.viewEntityProfile', { name: displayName })} mouseEnterDelay={1}>
                <LinkButton
                    to={entityUrl}
                    onClick={(e) => {
                        // Following the link shouldn't also toggle the row's browse filter.
                        e.stopPropagation();
                        trackEntityLinkClickEvent(targetNode);
                    }}
                    aria-label={t('sidebar.viewEntityProfile', { name: displayName })}
                >
                    <ArrowSquareOut size={14} />
                </LinkButton>
            </Tooltip>
        </span>
    );
};

export default EntityLink;
