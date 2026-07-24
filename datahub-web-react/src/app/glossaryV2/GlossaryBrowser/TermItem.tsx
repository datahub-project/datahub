import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import { EDITING_DOCUMENTATION_URL_PARAM } from '@app/entityV2/shared/constants';
import { useGlossaryActiveTabPath } from '@app/entityV2/shared/containers/profile/utils';
import {
    TreeRowContainer,
    TreeRowIconSlot,
    TreeRowLeftContent,
    TreeRowTitle,
} from '@app/glossaryV2/GlossaryBrowser/treeRow.styles';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { resolveGlossaryEntityColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { getGlossaryEntityIcon } from '@app/glossaryV2/utils';
import { SelectedMark } from '@app/sharedV2/icons/SelectedMark';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';
import { EntityType } from '@types';

// Row chrome (RowContainer/LeftContent/IconSlot/Title) lives in `treeRow.styles.ts`
// — shared with `NodeItem` so the two leaf-row types stay visually identical.

const TitleContent = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    flex: 1;
    overflow: hidden;
`;

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
}

function TermItem(props: Props) {
    const { term, isSelecting, selectTerm, includeActiveTabPath, depth, selectedUrns, iconColor } = props;

    const history = useHistory();
    const { entityData } = useGlossaryEntityData();
    const entityRegistry = useEntityRegistry();
    const activeTabPath = useGlossaryActiveTabPath();
    const generateColor = useGenerateGlossaryColorFromPalette();

    // Canonical resolver: term's own colorHex → inherited (passed by the parent NodeItem) →
    // parentNodes-derived → palette of the term's URN. Keeps the sidebar in sync with the entity
    // header, list cards, and modal picker.
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

    // Picker variant (AddRelatedTermsModal etc.) selects the term; otherwise
    // the row navigates to the term's entity page, preserving the active
    // tab path when the user isn't currently editing documentation.
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

    // Prefer the profile page's live (post-mutation) deprecation state over the sidebar's own
    // fetch when this row is the currently-open entity, mirroring the same isOnEntityPage
    // pattern already used above for the display name.
    const deprecation = isOnEntityPage ? entityData?.deprecation : term.deprecation;

    const deprecationBadge = deprecation?.deprecated && (
        <DeprecationSlot>
            <DeprecationIcon urn={term.urn} deprecation={deprecation} showUndeprecate={false} showText={false} />
        </DeprecationSlot>
    );

    return (
        <TreeRowContainer
            $level={depth}
            $isSelected={isRowSelected}
            onClick={handleRowClick}
            data-testid={`glossary-sidebar-term-${term.urn}`}
        >
            <TreeRowLeftContent>
                <TreeRowIconSlot>
                    <GlossaryColoredIcon color={resolvedIconColor} icon={TermIcon} size={20} iconSize={12} />
                </TreeRowIconSlot>
                <TitleContent>
                    <TreeRowTitle $isSelected={isRowSelected}>{displayName}</TreeRowTitle>
                    {deprecationBadge}
                </TitleContent>
            </TreeRowLeftContent>
            {isMultiSelected && <SelectedMark />}
        </TreeRowContainer>
    );
}

export default TermItem;
