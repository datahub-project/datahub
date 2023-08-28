import { Typography } from 'antd';
import React, { useEffect } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { Domain } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { RotatingTriangle } from '../../../shared/sidebar/components';
import DomainIcon from '../../DomainIcon';
import useListDomains from '../../useListDomains';
import useToggle from '../../../shared/useToggle';
import { BodyContainer, BodyGridExpander } from '../../../shared/components';
import { ANTD_GRAY_V2 } from '../../../entity/shared/constants';
import { useDomainsContext } from '../../DomainsContext';
import { applyOpacity } from '../../../shared/styleUtils';
import { useGetDomainChildrenCountLazyQuery } from '../../../../graphql/domain.generated';

const RowWrapper = styled.div`
    align-items: center;
    display: flex;
    padding: 2px 2px 4px 0;
    overflow: hidden;
`;

const NameWrapper = styled(Typography.Text)<{ isSelected: boolean; addLeftPadding: boolean }>`
    flex: 1;
    overflow: hidden;
    padding: 2px;
    ${(props) =>
        props.isSelected && `background-color: ${applyOpacity(props.theme.styles['primary-color'] || '', 10)};`}
    ${(props) => props.addLeftPadding && 'padding-left: 22px;'}

    &:hover {
        ${(props) => !props.isSelected && `background-color: ${ANTD_GRAY_V2[1]};`}
        cursor: pointer;
    }

    svg {
        margin-right: 6px;
    }
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
    padding-left: 24px;
`;

interface Props {
    domain: Domain;
    numDomainChildren: number;
    selectDomainOverride?: (urn: string, displayName: string) => void;
}

export default function DomainNode({ domain, numDomainChildren, selectDomainOverride }: Props) {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData, parentDomainsToUpate, setParentDomainsToUpdate } = useDomainsContext();
    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: false,
        closeDelay: 250,
    });
    const { data } = useListDomains({ parentDomain: domain.urn, skip: !isOpen });
    const [getDomainChildrenCount, { data: childrenData }] = useGetDomainChildrenCountLazyQuery();
    const isOnEntityPage = entityData && entityData.urn === domain.urn;
    const displayName = entityRegistry.getDisplayName(domain.type, isOnEntityPage ? entityData : domain);
    const isInSelectMode = !!selectDomainOverride;
    const hasDomainChildren = childrenData ? !!childrenData.domain?.children?.total : !!numDomainChildren;

    function handleSelectDomain() {
        if (selectDomainOverride) {
            selectDomainOverride(domain.urn, displayName);
        } else {
            history.push(entityRegistry.getEntityUrl(domain.type, domain.urn));
        }
    }

    useEffect(() => {
        // fetch updated children count to determine if we show triangle toggle
        if (parentDomainsToUpate.includes(domain.urn)) {
            setTimeout(() => {
                getDomainChildrenCount({ variables: { urn: domain.urn } });
                setParentDomainsToUpdate(parentDomainsToUpate.filter((urn) => urn !== domain.urn));
            }, 2000);
        }
    });

    return (
        <>
            <RowWrapper>
                {hasDomainChildren && (
                    <ButtonWrapper>
                        <RotatingTriangle isOpen={isOpen && !isClosing} onClick={toggle} />
                    </ButtonWrapper>
                )}
                <NameWrapper
                    ellipsis={{ tooltip: displayName }}
                    onClick={handleSelectDomain}
                    isSelected={!!isOnEntityPage && !isInSelectMode}
                    addLeftPadding={!hasDomainChildren}
                >
                    {!isInSelectMode && <DomainIcon />}
                    {displayName}
                </NameWrapper>
            </RowWrapper>
            <StyledExpander isOpen={isOpen && !isClosing}>
                <BodyContainer style={{ overflow: 'hidden' }}>
                    {data?.listDomains?.domains.map((childDomain) => (
                        <DomainNode
                            key={domain.urn}
                            domain={childDomain as Domain}
                            numDomainChildren={childDomain.children?.total || 0}
                            selectDomainOverride={selectDomainOverride}
                        />
                    ))}
                </BodyContainer>
            </StyledExpander>
        </>
    );
}
