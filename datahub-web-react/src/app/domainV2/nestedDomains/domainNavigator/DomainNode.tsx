import { Typography } from 'antd';
import { Tooltip } from '@components';
import React, { useEffect, useMemo } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { Domain } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { RotatingTriangle } from '../../../sharedV2/sidebar/components';
import useListDomains from '../../useListDomains';
import useToggle from '../../../shared/useToggle';
import { BodyContainer, BodyGridExpander } from '../../../shared/components';
import { useDomainsContext as useDomainsContextV2 } from '../../DomainsContext';
import { DomainColoredIcon } from '../../../entityV2/shared/links/DomainColoredIcon';
import { REDESIGN_COLORS, SEARCH_COLORS } from '../../../entityV2/shared/constants';

const Count = styled.div`
    color: ${REDESIGN_COLORS.BLACK};
    font-size: 12px;
    padding-left: 8px;
    padding-right: 8px;
    margin-left: 8px;
    border-radius: 11px;
    background-color: ${REDESIGN_COLORS.SIDE_BAR};
    width: 20%;
    height: 22px;
    display: flex;
    align-items: center;
    justify-content: center;
    max-width: 32px;
    transition: opacity 0.3s ease; /* add a smooth transition effect */
`;

const NameWrapper = styled(Typography.Text)<{ $isSelected: boolean; $addLeftPadding: boolean }>`
    flex: 1;
    padding: 2px;
    ${(props) => props.$isSelected && `color: ${SEARCH_COLORS.TITLE_PURPLE};`}
    ${(props) => props.$addLeftPadding && 'padding-left: 20px;'}

    &:hover {
        cursor: pointer;
    }
    display: flex !important;
    align-items: center;
    justify-content: space-between;
    transition: font-weight 0.3s ease-out;
    width: 100%;
`;

const DisplayName = styled.span<{ $isSelected: boolean }>`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: ${(props) => (props.$isSelected ? REDESIGN_COLORS.TITLE_PURPLE : REDESIGN_COLORS.BODY_TEXT_GREY)};
`;

const ButtonWrapper = styled.span<{ $addLeftPadding: boolean; $isSelected: boolean }>`
    margin-right: 4px;
    font-size: 16px;

    svg {
        font-size: 16px !important;
        color: ${(props) =>
            props.$isSelected ? REDESIGN_COLORS.TITLE_PURPLE : REDESIGN_COLORS.BODY_TEXT_GREY} !important;
    }

    .ant-btn {
        height: 16px;
        width: 16px;
    }
`;

const RowWrapper = styled.div<{ $isSelected: boolean; isOpen?: boolean }>`
    align-items: center;
    display: flex;
    width: 100%;
    border-bottom: 1px solid ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};
    padding: 12px;
    ${(props) => props.isOpen && `background-color: ${REDESIGN_COLORS.SECTION_BACKGROUND};`}
    ${(props) => props.$isSelected && `background-color: ${REDESIGN_COLORS.LIGHT_TEXT_DARK_BACKGROUND};`}
    &:hover {
        background-color: ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};
        ${ButtonWrapper} {
            svg {
                color: ${REDESIGN_COLORS.TITLE_PURPLE} !important;
            }
        }
        ${DisplayName} {
            color: ${REDESIGN_COLORS.TITLE_PURPLE};
        }
    }
`;

const StyledExpander = styled(BodyGridExpander)<{ paddingLeft: number }>`
    padding-left: 0px;
    background: ${REDESIGN_COLORS.SECTION_BACKGROUND};
    display: flex;
    width: 100%;
    overflow: auto;
    ${RowWrapper} {
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

interface Props {
    domain: Domain;
    numDomainChildren: number;
    isCollapsed?: boolean;
    domainUrnToHide?: string;
    selectDomainOverride?: (domain: Domain) => void;
    unhideSidebar?: () => void;
    $paddingLeft?: number;
}

export default function DomainNode({
    domain,
    numDomainChildren,
    domainUrnToHide,
    isCollapsed,
    selectDomainOverride,
    unhideSidebar,
    $paddingLeft = 0,
}: Props) {
    const shouldHideDomain = domainUrnToHide === domain.urn;
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData } = useDomainsContextV2();
    const { isOpen, isClosing, toggle, toggleOpen, toggleClose } = useToggle({
        initialValue: false,
        closeDelay: 250,
    });
    const { sortedDomains } = useListDomains({ parentDomain: domain.urn, skip: !isOpen || shouldHideDomain });
    const isOnEntityPage = entityData && entityData.urn === domain.urn;
    const displayName = entityRegistry.getDisplayName(domain.type, isOnEntityPage ? entityData : domain);
    const isInSelectMode = !!selectDomainOverride;
    const isDomainNodeSelected = !!isOnEntityPage && !isInSelectMode;
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

    function handleSelectDomain() {
        if (selectDomainOverride && !isCollapsed) {
            selectDomainOverride(domain);
        } else if (unhideSidebar && isCollapsed) {
            unhideSidebar();
        } else {
            history.push(entityRegistry.getEntityUrl(domain.type, domain.urn));
        }
    }

    if (shouldHideDomain) return null;

    const finalNumChildren = sortedDomains?.length ?? numDomainChildren;
    const hasDomainChildren = !!finalNumChildren;

    return (
        <>
            <RowWrapper
                data-testid="domain-list-item"
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
                        <Text>
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
                        {!isCollapsed && hasDomainChildren && <Count>{finalNumChildren}</Count>}
                    </NameWrapper>
                </Tooltip>
            </RowWrapper>
            <StyledExpander isOpen={isOpen && !isClosing} paddingLeft={paddingLeft}>
                <BodyContainer style={{ width: '100%' }}>
                    {sortedDomains?.map((childDomain) => (
                        <DomainNode
                            key={domain.urn}
                            domain={childDomain as Domain}
                            numDomainChildren={childDomain.children?.total || 0}
                            domainUrnToHide={domainUrnToHide}
                            selectDomainOverride={selectDomainOverride}
                            unhideSidebar={unhideSidebar}
                            $paddingLeft={paddingLeft}
                        />
                    ))}
                </BodyContainer>
            </StyledExpander>
        </>
    );
}
