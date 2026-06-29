import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { EDITING_DOCUMENTATION_URL_PARAM } from '@app/entityV2/shared/constants';
import { useGlossaryActiveTabPath } from '@app/entityV2/shared/containers/profile/utils';
import { SelectedMark } from '@app/glossaryV2/GlossaryBrowser/SelectedMark';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';

// --- Row chrome -------------------------------------------------------------
// Matches `NodeItem` (and through it, `DomainNode` / `DocumentTreeItem`) so
// every leaf row in the three tree sidebars shares the same anatomy. Terms
// never have an expand caret, so the icon slot always renders the colored
// glyph; everything else (38px row, 6px radius, brand-gradient selected,
// hover background + focus shadow, level-based indent) is identical.

const RowContainer = styled.div<{ $level: number; $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
    min-height: 38px;
    height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    margin: 0 2px 2px 2px;

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.bgSelectedSubtle};
        box-shadow: ${props.theme.colors.shadowFocusBrand};
    `}

    ${(props) =>
        !props.$isSelected &&
        `
        &:hover {
            background: ${props.theme.colors.bgHover};
            box-shadow: ${props.theme.colors.shadowFocus};
        }
    `}
`;

const LeftContent = styled.div`
    display: flex;
    align-items: center;
    flex: 1;
    min-width: 0;
    overflow: hidden;
`;

const IconSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    margin-right: 8px;
    flex-shrink: 0;
`;

const Title = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.brandGradientSelected};
        background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 600;
    `}
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

    // A term's own saved displayProperties.colorHex (set via the header color picker)
    // takes precedence over a color inherited from a parent node, which in turn beats the
    // deterministic palette fallback. This keeps the sidebar in sync with the entity header.
    const resolvedIconColor = term.displayProperties?.colorHex || iconColor || generateColor(term.urn);

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
        <RowContainer
            $level={depth}
            $isSelected={isRowSelected}
            onClick={handleRowClick}
            data-testid={`glossary-sidebar-term-${term.urn}`}
        >
            <LeftContent>
                <IconSlot>
                    <GlossaryColoredIcon color={resolvedIconColor} icon={BookmarkSimple} size={20} iconSize={12} />
                </IconSlot>
                <Title $isSelected={isRowSelected}>{displayName}</Title>
            </LeftContent>
            {isMultiSelected && <SelectedMark />}
        </RowContainer>
    );
}

export default TermItem;
