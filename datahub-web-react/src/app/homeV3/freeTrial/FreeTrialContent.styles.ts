import { Button, colors, Text, typography } from '@components';
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

export const TaskItem = styled.div<{ $isCompleted?: boolean }>`
    display: flex;
    align-items: center;
    padding: ${({ $isCompleted }) => ($isCompleted ? '12px 16px' : '12px 0')};
    margin: ${({ $isCompleted }) => ($isCompleted ? '0 -16px' : '0')};
    border-radius: 8px;
    background: ${({ $isCompleted }) => ($isCompleted ? colors.gray[1500] : 'transparent')};
    opacity: ${({ $isCompleted }) => ($isCompleted ? 0.7 : 1)};
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

