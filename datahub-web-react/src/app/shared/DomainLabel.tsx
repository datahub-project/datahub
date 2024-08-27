import React from 'react';
import styled from 'styled-components';

const DomainContainerWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px;
`;

const DomainContentWrapper = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

type Props = {
    name: string;
};

export const DomainLabel = ({ name }: Props) => {
    return (
        <DomainContainerWrapper>
            <DomainContentWrapper>
                <div>{name}</div>
            </DomainContentWrapper>
        </DomainContainerWrapper>
    );
};
