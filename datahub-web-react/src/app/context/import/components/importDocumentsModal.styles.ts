import { CheckCircleOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
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

export const SourceCard = styled.button`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 12px;
    padding: 24px 16px;
    border: 2px solid ${({ theme }) => theme.colors.border};
    border-radius: 8px;
    background: transparent;
    cursor: pointer;
    transition: all 0.15s ease;

    &:hover {
        border-color: ${({ theme }) => theme.colors.borderBrand};
    }
`;

export const SourceLogo = styled.img`
    width: 32px;
    height: 32px;
    object-fit: contain;
`;

export const SourceIcon = styled.div`
    color: ${({ theme }) => theme.colors.iconBrand};
`;

export const ResultContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 16px;
    padding: 20px 0;
`;

export const SuccessIcon = styled(CheckCircleOutlined)`
    font-size: 36px;
    color: ${({ theme }) => theme.colors.iconSuccess};
`;

export const WarningIcon = styled(ExclamationCircleOutlined)`
    font-size: 36px;
    color: ${({ theme }) => theme.colors.iconWarning};
`;

export const ErrorIcon = styled(ExclamationCircleOutlined)`
    font-size: 36px;
    color: ${({ theme }) => theme.colors.iconError};
`;

export const HelperText = styled.span`
    color: ${({ theme }) => theme.colors.textSecondary};
    font-size: 12px;
    text-align: center;
`;
