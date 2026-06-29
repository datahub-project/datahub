import { Pill, Tooltip } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled, { useTheme } from 'styled-components';

import { useDomainsContext as useDomainsContextV2 } from '@app/domainV2/DomainsContext';
import { useDomainSidebarFilters } from '@app/domainV2/nestedDomains/domainSidebarFilters/DomainSidebarFiltersContext';
import {
    extractDomainOwners,
    filterDomainsByOwner,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';
import { DomainNavigatorVariant } from '@app/domainV2/nestedDomains/types';
import useScrollDomains from '@app/domainV2/useScrollDomains';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import Loading from '@app/shared/Loading';
import { BodyContainer, BodyGridExpander } from '@app/shared/components';
import useToggle from '@app/shared/useToggle';
import { RotatingTriangle } from '@app/sharedV2/sidebar/components';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Domain } from '@types';

// --- Select-variant styled components ---------------------------------------
// These mirror the prior layout used by the "select" variant of the navigator
// (parent-domain picker in CreateDomainModal / DomainParentSelect). They are
// kept exactly as before so embeds outside the sidebar don't shift visually.

const NameWrapper = styled.div<{ $isSelected: boolean; $addLeftPadding: boolean }>`
    flex: 1;
    padding: 2px;
    ${(props) => props.$isSelected && `color: ${props.theme.colors.textSelected};`}
    ${(props) => props.$addLeftPadding && 'padding-left: 22px;'}

    &:hover {
        cursor: pointer;
    }
    display: flex;
    align-items: center;
    justify-content: space-between;
    transition: font-weight 0.3s ease-out;
    width: 100%;
`;

const DisplayName = styled.span<{ $isSelected: boolean }>`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: ${(props) => (props.$isSelected ? props.theme.colors.textSelected : props.theme.colors.textSecondary)};
`;

const ButtonWrapper = styled.span<{ $addLeftPadding: boolean; $isSelected: boolean }>`
    margin-right: 4px;
    font-size: 16px;

    svg {
        font-size: 16px !important;
        color: ${(props) =>
            props.$isSelected ? props.theme.colors.iconBrand : props.theme.colors.textSecondary} !important;
    }

    .ant-btn {
        height: 16px;
        width: 16px;
    }
`;

const SelectRowWrapper = styled.div<{ $isSelected: boolean; isOpen?: boolean }>`
    align-items: center;
    display: flex;
    width: 100%;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    padding: 12px;
    ${(props) => props.isOpen && `background-color: ${props.theme.colors.bgSurface};`}
    ${(props) => props.$isSelected && `background-color: ${props.theme.colors.bgSurfaceBrand};`}
    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
        ${ButtonWrapper} {
            svg {
                color: ${(props) => props.theme.colors.iconBrand} !important;
            }
        }
        ${DisplayName} {
            color: ${(props) => props.theme.colors.textHover};
        }
    }
`;

const StyledExpander = styled(BodyGridExpander)<{ paddingLeft: number }>`
    padding-left: 0px;
    background: ${(props) => props.theme.colors.bgSurface};
    display: flex;
    width: 100%;
    overflow: auto;
    ${SelectRowWrapper} {
        padding-left: ${(props) => props.paddingLeft + 12}px;
    }
`;

const Text = styled.div`
    display: flex;
    gap: 9px;
    align-items: center;
    font-size: 14px;
    width: 80%;
`;

const LoadingWrapper = styled.div`
    padding: 16px;
`;

// --- Sidebar-variant styled components --------------------------------------
// Layout mirrors `DocumentTreeItem` so the domain sidebar and document sidebar
// share the same row anatomy: 38px row, 6px radius, level-based left indent
// (8 + level*16), brand-gradient selected text, and the leading icon swapped
// for a caret on hover/expansion. Keeping these as a separate set of styled
// components (rather than reusing DocumentTreeItem) avoids dragging document-
// specific concerns — actions menu, dashed/external glyphs — into the domain
// tree row.

// `$isCollapsed` switches the row from the expanded "icon + title + count"
// layout to a single centered icon. Expanded mode keeps level-aware left
// padding (8 + level*16) so nested rows stair-step; collapsed mode drops the
// indent entirely and centers the icon in the 63px-wide collapsed sidebar so
// every row aligns vertically regardless of depth.
const SidebarRowContainer = styled.div<{ $level: number; $isSelected: boolean; $isCollapsed: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.$isCollapsed ? 'center' : 'space-between')};
    padding: ${(props) => (props.$isCollapsed ? '4px 0' : `4px 8px 4px ${8 + props.$level * 16}px`)};
    min-height: 38px;
    height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    margin-bottom: 2px;
    margin-left: 2px;
    margin-right: 2px;

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

// Expanded mode: flex:1 so the title pushes the right-side count to the edge.
// Collapsed mode: no flex (the icon centers itself via the parent's
// justify-content: center) and no overflow clipping needed.
const SidebarLeftContent = styled.div<{ $isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    ${(props) =>
        props.$isCollapsed
            ? `
        flex: 0 0 auto;
    `
            : `
        flex: 1;
        min-width: 0;
        overflow: hidden;
    `}
`;

// Drop the trailing 8px gutter in collapsed mode — with no title text next to
// it, the gutter would push the icon visibly right of center.
const SidebarIconSlot = styled.div<{ $isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    margin-right: ${(props) => (props.$isCollapsed ? '0' : '8px')};
    flex-shrink: 0;
`;

const SidebarExpandButton = styled.button`
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

const SidebarTitle = styled.span<{ $isSelected: boolean }>`
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

const SidebarRightContent = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-left: 8px;
    flex-shrink: 0;
`;

// Expander used by the sidebar variant. Unlike the select-variant expander,
// children are responsible for their own indent via the `level` prop, so this
// is just an animated open/close wrapper.
const SidebarExpander = styled(BodyGridExpander)`
    width: 100%;
    overflow: hidden;
`;

const SidebarLoadingWrapper = styled.div<{ $level: number }>`
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
`;

interface Props {
    domain: Domain;
    numDomainChildren: number;
    isCollapsed?: boolean;
    domainUrnToHide?: string;
    selectDomainOverride?: (domain: Domain) => void;
    $paddingLeft?: number;
    /**
     * Tree depth for the sidebar variant. Root rows are level 0; children get
     * `level + 1`. Drives the leading padding so nested rows visually nest
     * under their parent at the same cadence as DocumentTreeItem.
     */
    level?: number;
    variant?: DomainNavigatorVariant;
}

export default function DomainNode({
    domain,
    numDomainChildren,
    domainUrnToHide,
    isCollapsed,
    selectDomainOverride,
    $paddingLeft = 0,
    level = 0,
    variant = 'select',
}: Props) {
    const shouldHideDomain = domainUrnToHide === domain.urn;
    const { t: tc } = useTranslation('common.actions');
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData } = useDomainsContextV2();
    const { isOpen, isClosing, toggle, toggleOpen, toggleClose } = useToggle({
        initialValue: false,
        closeDelay: 250,
    });
    const { domains, loading, scrollRef } = useScrollDomains({
        parentDomain: domain.urn,
        skip: !isOpen || shouldHideDomain,
    });
    const theme = useTheme();
    const [isHovered, setIsHovered] = useState(false);
    const isOnEntityPage = entityData && entityData.urn === domain.urn;
    const displayName = entityRegistry.getDisplayName(domain.type, isOnEntityPage ? entityData : domain);
    const isInSelectMode = !!selectDomainOverride;
    const isSidebarVariant = variant === 'sidebar';
    const isDomainNodeSelected = !!isOnEntityPage && !isInSelectMode;
    const { selectedOwnerUrns, registerOwners } = useDomainSidebarFilters();
    const shouldAutoOpen = useMemo(
        () => !isInSelectMode && entityData?.parentDomains?.domains?.some((parent) => parent.urn === domain.urn),
        [isInSelectMode, entityData, domain.urn],
    );
    const paddingLeft = $paddingLeft + 16;

    useEffect(() => {
        if (shouldAutoOpen) toggleOpen();
    }, [shouldAutoOpen, toggleOpen]);

    useEffect(() => {
        if (isCollapsed) {
            toggleClose();
        }
    }, [isCollapsed, toggleClose]);

    // Register this domain's owners with the sidebar filter context so the
    // Owner multi-select can offer them as options. Only relevant in the
    // sidebar variant — the select variant is rendered inside pickers that
    // don't expose the owner filter.
    useEffect(() => {
        if (!isSidebarVariant) return;
        const owners = extractDomainOwners(domain);
        if (owners.length > 0) registerOwners(owners);
    }, [isSidebarVariant, domain, registerOwners]);

    // Filter loaded children at render time. Mirrors the per-level filter
    // model the documents sidebar uses (see `filterDocumentNodes`): each
    // expanded children list is filtered independently, so a parent can be
    // visible with no visible children, or vice versa.
    const visibleChildDomains = useMemo(() => {
        if (!isSidebarVariant) return domains || [];
        return filterDomainsByOwner(domains || [], selectedOwnerUrns);
    }, [isSidebarVariant, domains, selectedOwnerUrns]);

    function handleSelectDomain() {
        // Picker variant (CreateDomainModal / DomainParentSelect) takes
        // priority: clicking a row inside the picker should select the
        // domain, not navigate. Collapsed mode is irrelevant inside pickers.
        if (selectDomainOverride && !isCollapsed) {
            selectDomainOverride(domain);
            return;
        }
        // Sidebar mode (both collapsed and expanded): clicking a row
        // navigates to that domain's entity page. In collapsed mode we used
        // to just re-open the sidebar, but that swallowed the user's intent
        // — they're explicitly clicking a recognizable icon. Navigate
        // straight through.
        history.push(entityRegistry.getEntityUrl(domain.type, domain.urn));
    }

    if (shouldHideDomain) return null;

    const hasDomainChildren = !!numDomainChildren;
    const isExpanded = isOpen && !isClosing;

    // ------------------------------------------------------------------ sidebar
    if (variant === 'sidebar') {
        // In collapsed mode the row click expands the whole sidebar (not toggle
        // the node), so swapping the icon for a caret on hover would lie about
        // the click action. Lock to the colored icon while collapsed.
        const showCaret = !isCollapsed && hasDomainChildren && (isExpanded || isHovered);

        const handleCaretClick = (e: React.MouseEvent) => {
            e.stopPropagation();
            toggle();
        };

        const renderLeadingGlyph = () => {
            if (showCaret) {
                const Caret = isExpanded ? CaretDown : CaretRight;
                return (
                    <SidebarExpandButton
                        type="button"
                        onClick={handleCaretClick}
                        aria-expanded={isExpanded}
                        aria-label={isExpanded ? tc('collapse') : tc('expand')}
                        data-testid={`domain-tree-expand-button-${domain.urn}`}
                    >
                        <Caret color={theme.colors.icon} size={16} weight="bold" />
                    </SidebarExpandButton>
                );
            }
            // Domain glyph is intentionally smaller (20px) than the previous
            // 30px badge so it sits flush inside the 24x20 IconSlot used by
            // the documents sidebar — the two trees should look like siblings.
            return <DomainColoredIcon domain={domain} size={20} fontSize={12} />;
        };

        return (
            <>
                <SidebarRowContainer
                    data-testid="domain-options-list"
                    $level={level}
                    $isSelected={isDomainNodeSelected && !isCollapsed}
                    $isCollapsed={!!isCollapsed}
                    onClick={handleSelectDomain}
                    onMouseEnter={() => setIsHovered(true)}
                    onMouseLeave={() => setIsHovered(false)}
                >
                    <SidebarLeftContent $isCollapsed={!!isCollapsed}>
                        <SidebarIconSlot $isCollapsed={!!isCollapsed}>{renderLeadingGlyph()}</SidebarIconSlot>
                        {!isCollapsed && (
                            <Tooltip placement="right" title={displayName} mouseEnterDelay={0.7} mouseLeaveDelay={0}>
                                <SidebarTitle
                                    $isSelected={isDomainNodeSelected && !isCollapsed}
                                    data-testid={`domain-option-${displayName}`}
                                >
                                    {displayName}
                                </SidebarTitle>
                            </Tooltip>
                        )}
                    </SidebarLeftContent>
                    {hasDomainChildren && !isExpanded && !isCollapsed && (
                        <SidebarRightContent>
                            <Pill label={`${numDomainChildren}`} size="sm" />
                        </SidebarRightContent>
                    )}
                </SidebarRowContainer>
                <SidebarExpander isOpen={isExpanded}>
                    <BodyContainer>
                        {isExpanded && (
                            <>
                                {visibleChildDomains?.map((childDomain) => (
                                    <DomainNode
                                        key={childDomain.urn}
                                        domain={childDomain as Domain}
                                        numDomainChildren={childDomain.children?.total || 0}
                                        domainUrnToHide={domainUrnToHide}
                                        selectDomainOverride={selectDomainOverride}
                                        level={level + 1}
                                        variant={variant}
                                    />
                                ))}
                                {loading && (
                                    <SidebarLoadingWrapper $level={level + 1}>
                                        <Loading height={16} marginTop={0} />
                                    </SidebarLoadingWrapper>
                                )}
                                {visibleChildDomains.length > 0 && <div ref={scrollRef} />}
                            </>
                        )}
                    </BodyContainer>
                </SidebarExpander>
            </>
        );
    }

    // ------------------------------------------------------------------- select
    return (
        <>
            <SelectRowWrapper
                data-testid="domain-options-list"
                $isSelected={isDomainNodeSelected && !isCollapsed}
                isOpen={isOpen && !isClosing}
            >
                {!isCollapsed && hasDomainChildren && (
                    <ButtonWrapper
                        $addLeftPadding={!isCollapsed && !hasDomainChildren}
                        $isSelected={isDomainNodeSelected && !isCollapsed}
                    >
                        <RotatingTriangle
                            isOpen={isOpen && !isClosing}
                            onClick={toggle}
                            dataTestId="open-domain-item"
                        />
                    </ButtonWrapper>
                )}
                <Tooltip placement="right" title={displayName} mouseEnterDelay={0.7} mouseLeaveDelay={0}>
                    <NameWrapper
                        onClick={handleSelectDomain}
                        $isSelected={isDomainNodeSelected}
                        $addLeftPadding={!isCollapsed && !hasDomainChildren}
                    >
                        <Text data-testid={`domain-option-${displayName}`}>
                            <Tooltip
                                placement="right"
                                title={isCollapsed && displayName}
                                mouseEnterDelay={0.7}
                                mouseLeaveDelay={0}
                            >
                                <DomainColoredIcon domain={domain} size={30} fontSize={14} />
                            </Tooltip>
                            <DisplayName $isSelected={isDomainNodeSelected && !isCollapsed}>
                                {!isCollapsed && displayName}
                            </DisplayName>
                        </Text>
                        {!isCollapsed && hasDomainChildren && <Pill label={`${numDomainChildren}`} size="sm" />}
                    </NameWrapper>
                </Tooltip>
            </SelectRowWrapper>
            <StyledExpander isOpen={isOpen && !isClosing} paddingLeft={paddingLeft}>
                <BodyContainer style={{ width: '100%' }}>
                    {isOpen && (
                        <>
                            {domains?.map((childDomain) => (
                                <DomainNode
                                    key={childDomain.urn}
                                    domain={childDomain as Domain}
                                    numDomainChildren={childDomain.children?.total || 0}
                                    domainUrnToHide={domainUrnToHide}
                                    selectDomainOverride={selectDomainOverride}
                                    $paddingLeft={paddingLeft}
                                    variant={variant}
                                />
                            ))}
                            {loading && (
                                <LoadingWrapper>
                                    <Loading height={16} marginTop={0} />
                                </LoadingWrapper>
                            )}
                            {domains.length > 0 && <div ref={scrollRef} />}
                        </>
                    )}
                </BodyContainer>
            </StyledExpander>
        </>
    );
}
