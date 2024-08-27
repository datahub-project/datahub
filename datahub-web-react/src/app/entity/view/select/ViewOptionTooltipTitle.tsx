import React from 'react';
import styled from 'styled-components';

const NameContainer = styled.div`
    margin-bottom: 8px;
`;

type Props = {
    name: string;
    description?: string | null;
};

export const ViewOptionTooltipTitle = ({ name, description }: Props) => {
    return (
        <>
            <NameContainer>
                <b>{name}</b>
            </NameContainer>
            <div>{description}</div>
        </>
    );
};
