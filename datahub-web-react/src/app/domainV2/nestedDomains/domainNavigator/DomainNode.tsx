import { Typography } from 'antd';
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
import { applyOpacity } from '../../../shared/styleUtils';
import { DomainColoredIcon } from '../../../entityV2/shared/links/DomainColoredIcon';
import { REDESIGN_COLORS, SEARCH_COLORS } from '../../../entityV2/shared/constants';

const RowWrapper = styled.div<{ $isSelected: boolean }>`
    align-items: center;
    display: flex;
    padding: 12px;
    overflow: hidden;
    border-bottom: 1px solid #f0f0f0;
    ${(props) =>
        props.$isSelected && `background-color: ${applyOpacity(props.theme.styles['primary-color'] || '', 10)};`}
`;

const Count = styled.div`
    color: ${REDESIGN_COLORS.BLACK};
    font-size: 12px;
    padding-left: 8px;
    padding-right: 8px;
    margin-left: 8px;
    border-radius: 11px;
    background-color: ${REDESIGN_COLORS.SIDE_BAR};
    width: 100%;
    height: 22px;
    display: flex;
    align-items: center;
    justify-content: center;
    max-width: 32px;
    transition: opacity 0.3s ease; /* add a smooth transition effect */
`;

const NameWrapper = styled(Typography.Text)<{ $isSelected: boolean; $addLeftPadding: boolean }>`
    flex: 1;
    overflow: hidden;
    padding: 2px;
    ${(props) => props.$addLeftPadding && 'padding-left: 22px;'}
    ${(props) => props.$isSelected && `color: ${SEARCH_COLORS.TITLE_PURPLE}; font-weight: 700;`}
  &:hover {
        font-weight: 700;
        cursor: pointer;
    }
    display: flex !important;
    align-items: center;
    justify-content: space-between;
    transition: font-weight 0.3s ease-out;
`;

const ButtonWrapper = styled.span`
    margin-right: 4px;
    font-size: 16px;
    height: 16px;
    width: 16px;

    svg {
        height: 10px;
        width: 10px;
    }

    .ant-btn {
        height: 16px;
        width: 16px;
    }
`;

const StyledExpander = styled(BodyGridExpander)`
    padding-left: 0px;
    background: ${REDESIGN_COLORS.BACKGROUND_GRAY_2};
    ${RowWrapper} {
        border: none;
    }
    ${NameWrapper} {
        padding-left: 60px;
    }
`;

const Text = styled.div`
    display: flex;
    gap: 9px;
    align-items: center;
    font-size: 14px;
`;

interface Props {
    domain: Domain;
    numDomainChildren: number;
    domainUrnToHide?: string;
    selectDomainOverride?: (domain: Domain) => void;
}

export default function DomainNode({ domain, numDomainChildren, domainUrnToHide, selectDomainOverride }: Props) {
    const shouldHideDomain = domainUrnToHide === domain.urn;
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData } = useDomainsContextV2();
    const { isOpen, isClosing, toggle, toggleOpen } = useToggle({
        initialValue: false,
        closeDelay: 250,
    });
    const { sortedDomains } = useListDomains({ parentDomain: domain.urn, skip: !isOpen || shouldHideDomain });
    const isOnEntityPage = entityData && entityData.urn === domain.urn;
    const displayName = entityRegistry.getDisplayName(domain.type, isOnEntityPage ? entityData : domain);
    const isInSelectMode = !!selectDomainOverride;
    const isDomainNodeSelected = !!isOnEntityPage && !isInSelectMode;
    const shouldAutoOpen = useMemo(
        () => !isInSelectMode && entityData?.parentDomains?.domains.some((parent) => parent.urn === domain.urn),
        [isInSelectMode, entityData, domain.urn],
    );

    useEffect(() => {
        if (shouldAutoOpen) toggleOpen();
    }, [shouldAutoOpen, toggleOpen]);

    function handleSelectDomain() {
        if (selectDomainOverride) {
            selectDomainOverride(domain);
        } else {
            history.push(entityRegistry.getEntityUrl(domain.type, domain.urn));
        }
    }

    if (shouldHideDomain) return null;

    const finalNumChildren = sortedDomains?.length ?? numDomainChildren;
    return (
        <>
            <RowWrapper data-testid="domain-list-item" $isSelected={isDomainNodeSelected}>
                {!!finalNumChildren && (
                    <ButtonWrapper>
                        <RotatingTriangle isOpen={isOpen && !isClosing} onClick={toggle} />
                    </ButtonWrapper>
                )}
                <NameWrapper
                    ellipsis={{ tooltip: displayName }}
                    onClick={handleSelectDomain}
                    $isSelected={isDomainNodeSelected}
                    $addLeftPadding={!finalNumChildren}
                >
                    <Text>
                        <DomainColoredIcon
                            domain={domain}
                            size={28}
                            iconColor={isDomainNodeSelected ? SEARCH_COLORS.TITLE_PURPLE : undefined}
                        />
                        {displayName}
                    </Text>
                    <Count>{finalNumChildren}</Count>
                </NameWrapper>
            </RowWrapper>
            <StyledExpander isOpen={isOpen && !isClosing}>
                <BodyContainer style={{ overflow: 'hidden' }}>
                    {sortedDomains?.map((childDomain) => (
                        <DomainNode
                            key={domain.urn}
                            domain={childDomain as Domain}
                            numDomainChildren={childDomain.children?.total || 0}
                            domainUrnToHide={domainUrnToHide}
                            selectDomainOverride={selectDomainOverride}
                        />
                    ))}
                </BodyContainer>
            </StyledExpander>
        </>
    );
}
