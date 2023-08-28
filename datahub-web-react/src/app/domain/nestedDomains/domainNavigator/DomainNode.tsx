import { Typography } from 'antd';
import React from 'react';
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
}

export default function DomainNode({ domain, numDomainChildren }: Props) {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData } = useDomainsContext();
    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: false,
        closeDelay: 250,
    });
    const { data } = useListDomains({ parentDomain: domain.urn, skip: !isOpen });
    const isOnEntityPage = entityData && entityData.urn === domain.urn;
    const displayName = entityRegistry.getDisplayName(domain.type, isOnEntityPage ? entityData : domain);

    function handleSelectDomain() {
        history.push(entityRegistry.getEntityUrl(domain.type, domain.urn));
    }

    return (
        <>
            <RowWrapper>
                {!!numDomainChildren && (
                    <ButtonWrapper>
                        <RotatingTriangle isOpen={isOpen && !isClosing} onClick={toggle} />
                    </ButtonWrapper>
                )}
                <NameWrapper
                    ellipsis={{ tooltip: displayName }}
                    onClick={handleSelectDomain}
                    isSelected={!!isOnEntityPage}
                    addLeftPadding={!numDomainChildren}
                >
                    <DomainIcon />
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
                        />
                    ))}
                </BodyContainer>
            </StyledExpander>
        </>
    );
}
