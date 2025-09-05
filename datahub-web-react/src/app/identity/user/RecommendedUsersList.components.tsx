import { Text } from '@components';
import styled from 'styled-components';

import colors from '@src/alchemy-components/theme/foundations/colors';

export const RecommendedUsersContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-top: 24px;
    margin-bottom: 4px;
`;

export const UserCard = styled.div<{ $fadeOut?: boolean }>`
    display: flex;
    align-items: center;
    gap: 12px;
    background: ${colors.white};
    transition:
        opacity 0.3s ease-out,
        transform 0.3s ease-out;
    opacity: ${(props) => (props.$fadeOut ? 0 : 1)};
    transform: ${(props) => (props.$fadeOut ? 'translateY(-10px)' : 'translateY(0)')};
    ${(props) =>
        props.$fadeOut &&
        `
        pointer-events: none;
    `}
`;

export const UserInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
    flex: 1;
`;

export const UserDetails = styled.div`
    display: flex;
    flex-direction: column;
`;

export const UserEmail = styled(Text)`
    font-weight: 500;
`;

export const UserEmailRow = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
`;

export const UserTag = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-top: 4px;
`;

export const TagIcon = styled.div`
    font-size: 14px;
`;

export const EmptyMessage = styled(Text)`
    text-align: center;
    padding: 24px;
    color: ${colors.gray[1700]};
`;

export const RecommendedUsersHeader = styled(Text)`
    font-weight: 500;
    color: ${colors.gray[600]};
`;

export const PlatformPill = styled.div`
    display: inline-flex;
    align-items: center;
    background: ${colors.gray[1500]};
    border-radius: 12px;
    font-size: 11px;
    color: ${colors.gray[1700]};

    &:last-child {
        margin-right: 0;
    }
`;

export const PlatformIcon = styled.img`
    width: 12px;
    height: 12px;
    object-fit: contain;
`;

export const PlatformName = styled.span`
    font-size: 11px;
    color: ${colors.gray[1700]};
`;

export const TopUserPill = styled.div`
    display: inline-flex;
    align-items: center;
    padding: 2px 6px;
    background: ${colors.gray[1500]};
    border-radius: 12px;
    font-size: 11px;
    color: ${colors.gray[1700]};
    margin-right: 4px;
`;

export const InviteStatus = styled.div<{ $failed?: boolean }>`
    font-size: 12px;
    font-weight: 500;
    color: ${(props) => (props.$failed ? colors.red[1000] : colors.green[500])};
`;
