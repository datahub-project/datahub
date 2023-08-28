import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Domain } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { RotatingTriangle } from '../../../shared/sidebar/components';
import DomainIcon from '../../DomainIcon';
import useListDomains from '../../useListDomains';
import useToggle from '../../../shared/useToggle';
import { BodyContainer, BodyGridExpander } from '../../../shared/components';

const RowWrapper = styled.div`
    align-items: center;
    display: flex;
    padding: 3px 2px 4px 0;
    overflow: hidden;
`;

const NameWrapper = styled(Typography.Text)`
    flex: 1;
    overflow: hidden;
    svg {
        margin-right: 6px;
    }
`;

const ButtonWrapper = styled.span`
    margin-right: 4px;
`;

const StyledExpander = styled(BodyGridExpander)`
    padding-left: 24px;
`;

interface Props {
    domain: Domain;
}

export default function DomainNode({ domain }: Props) {
    const entityRegistry = useEntityRegistry();
    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: false,
        closeDelay: 250,
    });
    const { data } = useListDomains({ parentDomain: domain.urn, skip: !isOpen });

    return (
        <>
            <RowWrapper>
                {/* TODO: only show this triangle if we know there are child domains */}
                <ButtonWrapper>
                    <RotatingTriangle isOpen={isOpen && !isClosing} onClick={toggle} />
                </ButtonWrapper>
                <NameWrapper ellipsis={{ tooltip: entityRegistry.getDisplayName(domain.type, domain) }}>
                    <DomainIcon />
                    {entityRegistry.getDisplayName(domain.type, domain)}
                </NameWrapper>
            </RowWrapper>
            <StyledExpander isOpen={isOpen && !isClosing}>
                <BodyContainer>
                    {data?.listDomains?.domains.map((childDomain) => (
                        <DomainNode key={domain.urn} domain={childDomain as Domain} />
                    ))}
                </BodyContainer>
            </StyledExpander>
        </>
    );
}
