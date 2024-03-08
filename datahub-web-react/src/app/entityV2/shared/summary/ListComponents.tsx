import React from 'react';
import styled from 'styled-components';

export const HorizontalList = styled.div<{ gap?: number }>`
    display: flex;
    flex-direction: row;
    gap: ${({ gap }) => gap || 12}px;
    padding-right: min(10%, 40px);
    overflow-x: auto;
    overflow-y: hidden;
    scroll-behavior: smooth;
    white-space: nowrap;
    width: 100%;

    &::-webkit-scrollbar {
        display: none;
    }

    mask-image: linear-gradient(
        to left,
        rgba(255, 255, 255, 0),
        rgba(255, 255, 255, 0.5) 10px,
        rgba(255, 255, 255, 1) max(10%, 20px)
    );
`;

const HorizontalListEnder = styled.div`
    flex-grow: 10;
    width: 0;
`;

const SummaryColumnsWrapper = styled.div`
    display: flex;
    flex-direction: row;
    max-width: 100%;
    overflow: hidden;
    white-space: nowrap;
    width: 100%;
`;

const SummaryColumn = styled.div<{ n: number }>`
    display: flex;
    flex-direction: column;
    flex-grow: 1;
    max-width: ${({ n }) => 10 + 100 / n}%; // TODO: Use flexbox instead
`;

const VerticalDivider = styled.hr<{ margin: number }>`
    align-self: stretch;
    height: auto;
    flex-grow: 0;
    margin: 0 ${({ margin }) => margin}px;
    border: 0.5px solid;
    opacity: 0.1;
    vertical-align: text-top;
`;

export function SummaryColumns({ children }: { children: React.ReactNode[] }) {
    return (
        <SummaryColumnsWrapper>
            {children
                .filter((c) => !!c)
                .map((child, i) => (
                    <>
                        {i !== 0 && <VerticalDivider margin={20} />}
                        <SummaryColumn n={children.length}>{child}</SummaryColumn>
                    </>
                ))}
            <HorizontalListEnder />
        </SummaryColumnsWrapper>
    );
}
