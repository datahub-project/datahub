import styled from 'styled-components';

import colors from '@src/alchemy-components/theme/foundations/colors';

export const RecommendedUsersContainer = styled.div`
    display: flex;
    flex-direction: column;

    margin-top: 16px;
`;

export const RecommendedUsersHeader = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

export const FiltersHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 14px;
    margin-bottom: 16px;
`;

export const SearchContainer = styled.div`
    display: flex;
    align-items: center;
`;

export const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    max-height: calc(100vh - 400px);
    overflow: auto;

    /* Make table header sticky */
    .ant-table-thead {
        position: sticky;
        top: 0;
        z-index: 1;
        background: white;
    }

    /* Ensure header cells have proper background */
    .ant-table-thead > tr > th {
        background: white !important;
        border-bottom: 1px solid #f0f0f0;
    }
`;

export const ActionsContainer = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    justify-content: flex-end;
`;

export const PlatformPill = styled.div`
    display: inline-flex;
    align-items: center;
    background: ${colors.gray[1500]};
    border-radius: 12px;
    padding: 4px 6px;
    cursor: pointer;

    &:hover {
        background: ${colors.gray[1400]};
    }
`;

export const PlatformIcon = styled.img`
    width: 16px;
    height: 16px;
    object-fit: contain;
`;

export const RecommendationPill = styled.div`
    display: inline-flex;
    align-items: center;
    padding: 4px 8px;
    background: ${colors.gray[1500]};
    border-radius: 12px;
    font-size: 11px;
    color: ${colors.gray[1700]};
    font-weight: 500;
`;

export const PlatformPillsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

export const UsageTooltipContent = styled.div`
    border-radius: 8px;
    padding: 12px;

    .platform-usage-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-top: 4px;

        .platform-info {
            display: flex;
            align-items: center;
            gap: 6px;
        }
    }
`;

export const FadingTableRow = styled.tr<{ $isHiding: boolean }>`
    transition: opacity 0.5s ease-out;
    opacity: ${({ $isHiding }) => ($isHiding ? 0 : 1)};
`;

export const RecommendedNoteContainer = styled.div`
    margin-top: 4px;
    margin-bottom: 14px;
`;

export const HeaderSection = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
`;

export const UserAvatarSection = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

export const PlatformUsageRow = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 4px;
`;

export const PlatformInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
`;

export const TooltipContainer = styled.div`
    margin-top: 8px;
`;

export const ActionsButtonGroup = styled.div`
    display: flex;
    gap: 8px;
`;

export const EmptyStateContainer = styled.div`
    text-align: center;
    padding: 32px;
`;

export const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    margin-top: 24px;
`;
