import styled from 'styled-components';

export const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 20px;
    min-height: 200px;
`;

export const SourceGrid = styled.div<{ $columns: number }>`
    display: grid;
    grid-template-columns: repeat(${({ $columns }) => $columns}, 1fr);
    gap: 12px;
`;

export const ResultContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 16px;
    padding: 20px 0;
`;

export const HelperText = styled.span`
    color: ${({ theme }) => theme.colors.textSecondary};
    font-size: 12px;
    text-align: center;
`;
