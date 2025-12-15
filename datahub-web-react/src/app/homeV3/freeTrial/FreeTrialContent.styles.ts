import { Button, Text, colors, typography } from '@components';
import styled from 'styled-components';

export const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 0 42px;
`;

export const CardTitle = styled(Text)`
    font-size: ${typography.fontSizes.lg};
    font-weight: ${typography.fontWeights.bold};
    color: ${colors.gray[600]};
`;

export const CardDescription = styled(Text)`
    color: ${colors.gray[1700]};
    line-height: 1.5;
    margin-top: 16px;
`;

// Get Started Card Styles
export const GetStartedCard = styled.div`
    background: ${colors.white};
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
    padding: 20px;
`;

export const GetStartedHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 16px;
`;

export const HeaderContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

export const GetStartedTitle = styled(Text)`
    font-size: ${typography.fontSizes.lg};
    font-weight: ${typography.fontWeights.bold};
    color: ${colors.gray[600]};
`;

export const GetStartedSubtitle = styled(Text)`
    color: ${colors.gray[1700]};
`;

export const ProgressSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 20px;
`;

export const ProgressHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

export const ProgressLabel = styled(Text)`
    color: ${colors.gray[1700]};
    font-size: ${typography.fontSizes.sm};
    font-weight: ${typography.fontWeights.semiBold};
`;

export const ProgressCount = styled(Text)`
    color: ${colors.gray[1700]};
    font-size: ${typography.fontSizes.sm};
    font-weight: ${typography.fontWeights.semiBold};
`;

export const ProgressBarContainer = styled.div`
    width: 100%;
    height: 6px;
    background: ${colors.gray[100]};
    border-radius: 3px;
    overflow: hidden;
`;

export const ProgressBarFill = styled.div<{ $percent: number }>`
    height: 100%;
    width: ${({ $percent }) => $percent}%;
    background: linear-gradient(90deg, ${colors.violet[500]} 0%, ${colors.blue[400]} 100%);
    border-radius: 3px;
    transition: width 0.3s ease;
`;

export const TaskList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

export const TaskItem = styled.div`
    display: flex;
    align-items: center;
    padding: 12px 0;
`;

export const TaskIconWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 40px;
    height: 40px;
    border-radius: 200px;
    background: ${colors.violet[0]};
    margin-right: 16px;
    flex-shrink: 0;
`;

export const TaskContent = styled.div`
    flex: 1;
    min-width: 0;
`;

export const TaskTitle = styled(Text)`
    font-weight: ${typography.fontWeights.semiBold};
    color: ${colors.gray[600]};
    margin-bottom: 2px;
`;

export const TaskDescription = styled(Text)`
    color: ${colors.gray[1700]};
    font-size: ${typography.fontSizes.sm};
    line-height: 1.4;
`;

export const TaskActions = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-left: 16px;
    flex-shrink: 0;
`;

export const DismissButton = styled(Text)`
    color: ${colors.gray[1700]};
    cursor: pointer;
    font-size: ${typography.fontSizes.sm};

    &:hover {
        color: ${colors.gray[600]};
    }
`;

export const MenuButton = styled(Button)`
    padding: 4px;
`;

export const MenuItem = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 2px 12px;
    cursor: pointer;
    color: ${colors.gray[600]};
    font-size: ${typography.fontSizes.md};

    &:hover {
        background: ${colors.gray[100]};
    }
`;

// Completion State Styles
export const CompletionContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 40px 20px;
    text-align: center;
`;

export const CompletionIconWrapper = styled.div`
    margin-bottom: 16px;
`;

export const CompletionTitle = styled(Text)`
    font-size: ${typography.fontSizes.lg};
    font-weight: ${typography.fontWeights.bold};
    color: ${colors.gray[600]};
    margin-bottom: 8px;
`;

export const CompletionSubtitle = styled(Text)`
    color: ${colors.gray[1700]};
    margin-bottom: 24px;
`;

export const CompletionActions = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 12px;
`;

export const CompletionLink = styled(Text)`
    color: ${colors.violet[500]};
    cursor: pointer;
    font-size: ${typography.fontSizes.sm};

    &:hover {
        text-decoration: underline;
    }
`;
