import { Avatar, Button } from '@components';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import DomainSearch from '@app/domainV2/DomainSearch';
import DomainsSidebarHeader from '@app/domainV2/nestedDomains/DomainsSidebarHeader';
import DomainNavigator from '@app/domainV2/nestedDomains/domainNavigator/DomainNavigator';
import {
    DomainSidebarFiltersProvider,
    useDomainSidebarFilters,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/DomainSidebarFiltersContext';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { EntityType } from '@types';

const PLATFORM_BROWSE_TRANSITION_MS = 300;

// TODO: Clean up how we do expand / collapse
const StyledEntitySidebarContainer = styled.div<{
    isCollapsed: boolean;
    $width?: number;
    backgroundColor?: string;
    $isShowNavBarRedesign?: boolean;
    $isEntityProfile?: boolean;
}>`
    flex-shrink: 0;
    max-height: 100%;

    width: ${(props) => (props.isCollapsed ? '63px' : `${props.$width}px`)};
    margin-bottom: ${(props) => (props.$isShowNavBarRedesign ? '0' : '12px')};
    transition: width ${PLATFORM_BROWSE_TRANSITION_MS}ms ease-in-out;

    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex-direction: column;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
 margin: ${props.$isEntityProfile ? '5px 12px 5px 5px' : '0 16px 0 0'};
 box-shadow: ${props.theme.colors.shadowNavbar};
 `}
`;

const Controls = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.isCollapsed ? 'center' : 'space-between')};
    padding: 12px;
    overflow: hidden;
    height: 50px;
`;

// Thin horizontal rule used between the sidebar header and the search/tree
// region. Plain styled div (vs. antd's Divider) so this file is antd-free.
const ThinDivider = styled.div`
    height: 0;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const StyledSidebar = styled.div`
    overflow: auto;
    height: 100%;
    display: flex;
    flex-direction: column;
`;

// Filter row mirrors the FiltersRow in ContextSidebar: a single-row wrap of
// pill-shaped SimpleSelects packed to the start of the row. Padding matches
// the search bar's InputWrapper above so the row aligns.
const FiltersRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    padding: 0 12px 12px 12px;
`;

/**
 * Multi-row option layout for the Owner filter dropdown. SimpleSelect doesn't
 * render avatars in multi-select option rows natively, so the Owner filter
 * supplies its own `renderCustomOptionText` — this is the layout it returns.
 */
const OwnerOptionRow = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

type Props = {
    isEntityProfile?: boolean;
};

/**
 * Inner sidebar — consumes the filters context so the Owner SimpleSelect can
 * source its options from owners discovered across the loaded domain tree.
 * Split from the default export so the provider can wrap it without the inner
 * component having to thread props down.
 */
function ManageDomainsSidebarInner({ isEntityProfile }: Props) {
    const { t } = useTranslation('governance.domain');
    const width = useSidebarWidth(0.2);
    const [isClosed, setIsClosed] = useState(false);
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { selectedOwnerUrns, setSelectedOwnerUrns, availableOwners } = useDomainSidebarFilters();

    const unhideSidebar = useCallback(() => {
        setIsClosed(false);
    }, []);

    // SimpleSelect option list is built from the distinct owners that the
    // tree has reported as it rendered. Each option carries the owner record
    // so the custom renderer can pull avatar fields without re-deriving.
    const ownerOptions = useMemo(
        () =>
            availableOwners.map((owner) => ({
                value: owner.urn,
                label: owner.displayName,
                owner,
            })),
        [availableOwners],
    );

    return (
        <StyledEntitySidebarContainer
            isCollapsed={isClosed}
            $width={width}
            id="browse-v2"
            $isShowNavBarRedesign={isShowNavBarRedesign}
            $isEntityProfile={isEntityProfile}
        >
            <Controls isCollapsed={isClosed}>
                {!isClosed && <DomainsSidebarHeader />}
                <Button
                    variant="text"
                    color="gray"
                    size="lg"
                    isCircle
                    icon={{ icon: isClosed ? ArrowLineRight : ArrowLineLeft, color: 'icon' }}
                    isActive={!isClosed}
                    onClick={() => setIsClosed(!isClosed)}
                />
            </Controls>
            <ThinDivider />
            <StyledSidebar>
                <DomainSearch isCollapsed={isClosed} unhideSidebar={unhideSidebar} />
                {!isClosed && (
                    <FiltersRow>
                        <SimpleSelect
                            size="sm"
                            width="fit-content"
                            isMultiSelect
                            showSearch
                            filterResultsByQuery
                            isDisabled={ownerOptions.length === 0}
                            placeholder={t('navigator.ownerFilter.placeholder')}
                            selectLabelProps={{ variant: 'labeled', label: t('navigator.ownerFilter.label') }}
                            options={ownerOptions}
                            values={selectedOwnerUrns}
                            onUpdate={setSelectedOwnerUrns}
                            renderCustomOptionText={(option) => {
                                const { owner } = option as (typeof ownerOptions)[number];
                                return (
                                    <OwnerOptionRow>
                                        <Avatar
                                            name={owner.displayName}
                                            imageUrl={owner.pictureLink ?? undefined}
                                            type={
                                                owner.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user
                                            }
                                            showInPill
                                            size="sm"
                                        />
                                    </OwnerOptionRow>
                                );
                            }}
                            dataTestId="domain-sidebar-owner-filter"
                        />
                    </FiltersRow>
                )}
                {/* Full-width divider sits between the filters and the navigator,
                    matching the documents sidebar's separator placement (above
                    the "home" / overview row, below the filter pills). */}
                {!isClosed && <ThinDivider />}
                <DomainNavigator isCollapsed={isClosed} variant="sidebar" />
            </StyledSidebar>
        </StyledEntitySidebarContainer>
    );
}

export default function ManageDomainsSidebarV2(props: Props) {
    return (
        <DomainSidebarFiltersProvider>
            <ManageDomainsSidebarInner {...props} />
        </DomainSidebarFiltersProvider>
    );
}
