import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const TopRow = styled.div`
    display: flex;
    flex-direction: row;
    margin-top: 12px;
`;

const Spacer = styled.div`
    flex: 1;
`;

const LeftSection = styled.div<{ $isClickable?: boolean }>`
    display: flex;
    align-items: center;
    gap: 8px;

    ${(props) =>
        props.$isClickable &&
        `
        :hover {
            cursor: pointer;    
        }
        `}
`;

interface Props {
    name: string;
    description?: string;
    topRowRightItems?: React.ReactNode;
    topRowLeftItems?: React.ReactNode;
    onHeaderClick?: () => void;
}

export function SectionName({ name, description, topRowRightItems, topRowLeftItems, onHeaderClick }: Props) {
    return (
        <Wrapper>
            <TopRow>
                <LeftSection onClick={onHeaderClick} $isClickable={!!onHeaderClick}>
                    {topRowLeftItems}
                    <Text weight="semiBold" size="lg">
                        {name}
                    </Text>
                </LeftSection>
                <Spacer />
                {topRowRightItems}
            </TopRow>
            {description && <Text color="gray">{description}</Text>}
        </Wrapper>
    );
}
