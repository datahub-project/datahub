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
    margin-bottom: 16px;
    overflow: hidden;

    transition:
        opacity 0.8s ease-out,
        transform 0.8s ease-out,
        max-height 0.8s ease-out,
        margin 0.8s ease-out;

    /* Animation states for smooth fadeout and slide up */
    opacity: ${(props) => (props.$fadeOut ? 0 : 1)};
    transform: ${(props) => (props.$fadeOut ? 'translateY(-10px)' : 'translateY(0)')};
    max-height: ${(props) => (props.$fadeOut ? '0' : '80px')};
    margin-bottom: ${(props) => (props.$fadeOut ? '0' : '16px')};

    ${(props) =>
        props.$fadeOut &&
        `
        pointer-events: none;
    `}
`;

export const UserInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex: 1;
`;

export const UserDetails = styled.div`
    display: flex;
    flex-direction: column;
    min-width: 0;
    flex: 1;
`;

export const UserEmail = styled(Text)`
    max-width: 320px;
    color: ${colors.gray[600]};
    line-height: normal;
    text-overflow: ellipsis;
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
    flex-wrap: nowrap;
    overflow: hidden;
    width: 100%;
`;

export const TagIcon = styled.div`
    font-size: 14px;
`;

export const EmptyMessage = styled(Text)`
    text-align: center;
    padding: 24px;
    color: ${colors.gray[1700]};
`;

export const PlatformPillWrapper = styled.div`
    display: inline-flex;
    align-items: center;
    background: ${colors.gray[1500]};
    border-radius: 12px;
    font-size: 11px;
    color: ${colors.gray[1700]};
    padding: 2px 4px;
    flex-shrink: 0;

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

export const InviteStatus = styled.div<{ $failed?: boolean }>`
    font-size: 12px;
    font-weight: 500;
    color: ${(props) => (props.$failed ? colors.red[1000] : colors.green[500])};
`;

export const ViewMoreLink = styled.a`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 12px 16px;
    margin-top: 8px;
    border: 1px solid ${colors.gray[1500]};
    border-radius: 8px;
    background: ${colors.gray[100]};
    color: ${colors.blue[1800]};
    text-decoration: none;
    font-size: 14px;
    font-weight: 500;
    transition: all 0.2s ease;
    cursor: pointer;

    &:hover {
        background: ${colors.gray[300]};
        border-color: ${colors.blue[1800]};
        color: ${colors.blue[1900]};
        text-decoration: none;
    }
`;
