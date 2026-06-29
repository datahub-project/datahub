import React from 'react';
import { useHistory } from 'react-router-dom';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { EDITING_DOCUMENTATION_URL_PARAM } from '@app/entityV2/shared/constants';
import { useGlossaryActiveTabPath } from '@app/entityV2/shared/containers/profile/utils';
import { SelectedMark } from '@app/glossaryV2/GlossaryBrowser/SelectedMark';
import {
    TreeRowContainer,
    TreeRowIconSlot,
    TreeRowLeftContent,
    TreeRowTitle,
} from '@app/glossaryV2/GlossaryBrowser/treeRow.styles';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { getGlossaryEntityIcon } from '@app/glossaryV2/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';
import { EntityType } from '@types';

// Row chrome (RowContainer/LeftContent/IconSlot/Title) lives in `treeRow.styles.ts`
// — shared with `NodeItem` so the two leaf-row types stay visually identical.

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

    // Terms inherit the resolved color the parent NodeItem passed down (`iconColor`); if the
    // term has no parent (root-level term), fall back to a deterministic palette slot seeded
    // by its own URN.
    const resolvedIconColor = iconColor || generateColor(term.urn);
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
                <TreeRowTitle $isSelected={isRowSelected}>{displayName}</TreeRowTitle>
            </TreeRowLeftContent>
            {isMultiSelected && <SelectedMark />}
        </TreeRowContainer>
    );
}

export default TermItem;
