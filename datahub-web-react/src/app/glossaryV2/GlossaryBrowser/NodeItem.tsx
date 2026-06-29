import { Pill } from '@components';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled, { useTheme } from 'styled-components/macro';

import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { SelectedMark } from '@app/glossaryV2/GlossaryBrowser/SelectedMark';
import TermItem from '@app/glossaryV2/GlossaryBrowser/TermItem';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Loader } from '@src/alchemy-components';
import useGlossaryChildren from '@src/app/entityV2/glossaryNode/useGlossaryChildren';

import { GlossaryNodeFragment } from '@graphql/fragments.generated';
import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

// --- Row chrome -------------------------------------------------------------
// Matches `DomainNode` (and through it, `DocumentTreeItem`) so the three
// tree sidebars (Glossary, Domains, Documents) read as siblings: 38px row,
// 6px radius, brand-gradient text on selected, neutral hover background +
// focus shadow, level-based left indent (8 + depth * 16).

const ItemWrapper = styled.div`
    display: flex;
    flex-direction: column;
    position: relative;
`;

const RowContainer = styled.div<{ $level: number; $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    justify-content: space-between;
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

// 24x20 icon slot — same size and gutter `DomainNode` / `DocumentTreeItem`
// use, so vertical alignment matches across all three sidebars.
const IconSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    margin-right: 8px;
    flex-shrink: 0;
`;

// Caret button is rendered inside the IconSlot when the node has children
// and is either expanded or hovered, swapping the colored glossary glyph
// for an expand/collapse affordance. Border-less + transparent so only the
// caret itself is visible.
const ExpandButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: inherit;

    &:hover {
        opacity: 0.7;
    }
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

const RightContent = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-left: 8px;
    flex-shrink: 0;
`;

const ChildrenWrapper = styled.div``;

const LoadingWrapper = styled.div<{ $level: number }>`
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
`;

interface Props {
    node: GlossaryNodeFragment;
    isSelecting?: boolean;
    hideTerms?: boolean;
    openToEntity?: boolean;
    refreshBrowser?: boolean;
    nodeUrnToHide?: string;
    selectTerm?: (urn: string, displayName: string) => void;
    selectNode?: (urn: string, displayName: string) => void;
    depth: number;
    selectedUrns?: string[];
    iconColor?: string;
}

function NodeItem(props: Props) {
    const {
        node,
        isSelecting,
        hideTerms,
        openToEntity,
        refreshBrowser,
        nodeUrnToHide,
        selectTerm,
        selectNode,
        depth,
        selectedUrns,
        iconColor,
    } = props;
    const shouldHideNode = nodeUrnToHide === node.urn;

    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();
    const { entityData } = useGlossaryEntityData();

    const [areChildrenVisible, setAreChildrenVisible] = useState(false);
    const [isHovered, setIsHovered] = useState(false);

    const entityUrn = node.urn;
    const {
        scrollRef,
        data: children,
        loading,
    } = useGlossaryChildren({ entityUrn, skip: !areChildrenVisible || shouldHideNode });

    useEffect(() => {
        if (openToEntity && entityData && entityData.parentNodes?.nodes?.some((parent) => parent.urn === node.urn)) {
            setAreChildrenVisible(true);
        }
    }, [entityData, node.urn, openToEntity]);

    useEffect(() => {
        if (refreshBrowser) {
            setAreChildrenVisible(false);
        }
    }, [refreshBrowser]);

    const noOfChildren = (node.childrenCount?.termsCount || 0) + (node.childrenCount?.nodesCount || 0);
    const hasChildren = noOfChildren > 0;

    function handleSelectNode() {
        if (selectNode) {
            const displayName = entityRegistry.getDisplayName(node.type, node);
            selectNode(node.urn, displayName);
        }
    }

    // Picker variant (AddRelatedTermsModal etc.) selects the node; otherwise
    // the row navigates to the node's entity page. Mirrors `DomainNode`'s
    // pattern so both sidebars feel identical when used as pickers vs. nav.
    function handleRowClick() {
        if (isSelecting) {
            handleSelectNode();
            return;
        }
        history.push(entityRegistry.getEntityUrl(node.type, node.urn));
    }

    function handleCaretClick(e: React.MouseEvent) {
        e.stopPropagation();
        setAreChildrenVisible((v) => !v);
    }

    const childNodes = children
        ?.filter((child) => child?.type === EntityType.GlossaryNode)
        .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA, nodeB));
    const childTerms = children
        ?.filter((child) => child?.type === EntityType.GlossaryTerm)
        .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA, termB));

    const isMultiSelected = isSelecting && selectedUrns?.includes(node.urn);
    const isOnEntityPage = entityData?.urn === node.urn;
    const isRowSelected = !!isOnEntityPage && !isSelecting;

    if (shouldHideNode) return null;

    // A node's own saved displayProperties.colorHex (set via the header color picker)
    // takes precedence over a color inherited from a parent node, which in turn beats the
    // deterministic palette fallback. This keeps the sidebar in sync with the entity header.
    const glossaryColor = node.displayProperties?.colorHex || iconColor || generateColor(node.urn);

    const showCaret = hasChildren && (areChildrenVisible || isHovered);

    const renderLeadingGlyph = () => {
        if (showCaret) {
            const Caret = areChildrenVisible ? CaretDown : CaretRight;
            return (
                <ExpandButton
                    type="button"
                    onClick={handleCaretClick}
                    aria-expanded={areChildrenVisible}
                    aria-label={areChildrenVisible ? tc('collapse') : tc('expand')}
                    data-testid={`glossary-tree-expand-button-${node.urn}`}
                >
                    <Caret color={theme.colors.icon} size={16} weight="bold" />
                </ExpandButton>
            );
        }
        return <GlossaryColoredIcon color={glossaryColor} icon={BookmarksSimple} size={20} iconSize={12} />;
    };

    const displayName = entityRegistry.getDisplayName(node.type, node);

    return (
        <ItemWrapper>
            <RowContainer
                $level={depth}
                $isSelected={isRowSelected}
                onClick={handleRowClick}
                onMouseEnter={() => setIsHovered(true)}
                onMouseLeave={() => setIsHovered(false)}
                data-testid={`glossary-sidebar-node-${node.urn}`}
            >
                <LeftContent>
                    <IconSlot>{renderLeadingGlyph()}</IconSlot>
                    <Title $isSelected={isRowSelected}>{displayName}</Title>
                </LeftContent>
                {hasChildren && !areChildrenVisible && (
                    <RightContent>
                        <Pill label={`${noOfChildren}`} size="sm" />
                    </RightContent>
                )}
                {isMultiSelected && <SelectedMark />}
            </RowContainer>
            {areChildrenVisible && (
                <>
                    {!children.length && loading && (
                        <LoadingWrapper $level={depth + 1}>
                            <Loader size="xs" padding={0} />
                        </LoadingWrapper>
                    )}
                    {children.length > 0 && (
                        <ChildrenWrapper>
                            {(childNodes as GlossaryNode[]).map((child) => (
                                <NodeItem
                                    node={child}
                                    isSelecting={isSelecting}
                                    hideTerms={hideTerms}
                                    openToEntity={openToEntity}
                                    nodeUrnToHide={nodeUrnToHide}
                                    selectTerm={selectTerm}
                                    selectNode={selectNode}
                                    key={child.urn}
                                    depth={depth + 1}
                                    selectedUrns={selectedUrns}
                                    iconColor={glossaryColor}
                                />
                            ))}
                            {!hideTerms &&
                                (childTerms as GlossaryTerm[]).map((child) => (
                                    <TermItem
                                        key={child.urn}
                                        term={child}
                                        isSelecting={isSelecting}
                                        selectTerm={selectTerm}
                                        includeActiveTabPath
                                        depth={depth + 1}
                                        selectedUrns={selectedUrns}
                                        iconColor={glossaryColor}
                                    />
                                ))}
                            <div ref={scrollRef} />
                        </ChildrenWrapper>
                    )}
                </>
            )}
        </ItemWrapper>
    );
}

export default NodeItem;
