import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import { EDITING_DOCUMENTATION_URL_PARAM } from '@app/entityV2/shared/constants';
import { useGlossaryActiveTabPath } from '@app/entityV2/shared/containers/profile/utils';
import { SelectedMark } from '@app/glossaryV2/GlossaryBrowser/SelectedMark';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { resolveGlossaryEntityColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { getGlossaryEntityIcon } from '@app/glossaryV2/utils';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import {
    TREE_ROW_ENTITY_ICON_GLYPH_SIZE,
    TREE_ROW_ENTITY_ICON_SIZE,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';
import { EntityType } from '@types';

const DeprecationSlot = styled.span`
    display: inline-flex;
    align-items: center;
    flex-shrink: 0;
    line-height: 0;

    & svg {
        width: 12px;
        height: 12px;
    }
`;

interface Props {
    term: ChildGlossaryTermFragment;
    isSelecting?: boolean;
    selectTerm?: (urn: string, displayName: string) => void;
    includeActiveTabPath?: boolean;
    depth: number;
    selectedUrns?: string[];
    iconColor?: string;
    /**
     * SaaS: pass lifecycle / version badges here (or compose in a fork wrapper).
     * OSS uses this for deprecation; shared row never imports SaaS badge components.
     */
    afterLabel?: React.ReactNode;
}

function TermItem(props: Props) {
    const { term, isSelecting, selectTerm, includeActiveTabPath, depth, selectedUrns, iconColor, afterLabel } = props;

    const history = useHistory();
    const { entityData } = useGlossaryEntityData();
    const entityRegistry = useEntityRegistry();
    const activeTabPath = useGlossaryActiveTabPath();
    const generateColor = useGenerateGlossaryColorFromPalette();

    const resolvedIconColor = resolveGlossaryEntityColor(term, generateColor, { inheritedColor: iconColor });
    const TermIcon = getGlossaryEntityIcon(EntityType.GlossaryTerm);

    const isOnEntityPage = entityData?.urn === term.urn;
    const isMultiSelected = isSelecting && selectedUrns?.includes(term.urn);
    const isRowSelected = !!isOnEntityPage && !isSelecting;

    const isActivelyEditing = activeTabPath.includes(EDITING_DOCUMENTATION_URL_PARAM);

    function handleSelectTerm() {
        if (selectTerm) {
            const displayName = entityRegistry.getDisplayName(term.type, term);
            selectTerm(term.urn, displayName);
        }
    }

    function handleRowClick() {
        if (isSelecting) {
            handleSelectTerm();
            return;
        }
        const url = entityRegistry.getEntityUrl(term.type, term.urn);
        const suffix = includeActiveTabPath && !isActivelyEditing ? `/${activeTabPath}` : '';
        history.push(`${url}${suffix}`);
    }

    const displayName = entityRegistry.getDisplayName(term.type, isOnEntityPage ? entityData : term);

    const deprecation = isOnEntityPage ? entityData?.deprecation : term.deprecation;

    const deprecationBadge = deprecation?.deprecated ? (
        <DeprecationSlot>
            <DeprecationIcon urn={term.urn} deprecation={deprecation} showUndeprecate={false} showText={false} />
        </DeprecationSlot>
    ) : null;

    // SaaS can pass lifecycle/version via `afterLabel`; OSS deprecation composes with it.
    const resolvedAfterLabel =
        afterLabel != null || deprecationBadge != null ? (
            <>
                {deprecationBadge}
                {afterLabel}
            </>
        ) : undefined;

    return (
        <HierarchicalBrowseTreeRow
            level={depth}
            isSelected={isRowSelected}
            icon={
                <GlossaryColoredIcon
                    color={resolvedIconColor}
                    icon={TermIcon}
                    size={TREE_ROW_ENTITY_ICON_SIZE}
                    iconSize={TREE_ROW_ENTITY_ICON_GLYPH_SIZE}
                />
            }
            label={displayName}
            afterLabel={resolvedAfterLabel}
            trailing={isMultiSelected ? <SelectedMark /> : undefined}
            onSelect={handleRowClick}
            data-testid={`glossary-sidebar-term-${term.urn}`}
        />
    );
}

export default TermItem;
