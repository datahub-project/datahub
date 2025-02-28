import { colors, spacing } from '@src/alchemy-components/theme';
import styled from 'styled-components';

export const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    margin: ${spacing.md};
    color: ${colors.gray[1800]};

    .ant-pagination {
        li {
            margin-right: 4px;
        }
    }

    .ant-pagination-item {
        border: none;

        a {
            color: ${colors.gray[1800]};
        }
    }

    .ant-pagination-item-active {
        border-radius: 200px;
        background: ${colors.violet[0]};

        a {
            color: ${colors.violet[500]};
            font-weight: 700;
        }
    }

    button {
        color: ${colors.gray[1800]};
        border: 1px solid ${colors.gray[100]};
    }

    .ant-pagination-options {
        span,
        .ant-select-item {
            color: ${colors.gray[1800]};
        }
    }

    .ant-pagination-options-size-changer {
        .ant-select-selector {
            border: 1px solid ${colors.gray[100]};
        }

        &:hover:not(.ant-select-disabled),
        &.ant-select-focused:not(.ant-select-disabled) {
            .ant-select-selector {
                border: 1px solid ${colors.gray[100]};
                box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
            }
        }
    }

    .ant-pagination-next,
    .ant-pagination-prev {
        :hover {
            box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);

            button {
                color: ${colors.gray[1800]};
                border: 1px solid ${colors.gray[100]};
            }
        }
    }

    .ant-pagination-disabled:hover {
        box-shadow: none;
        button {
            color: rgba(0, 0, 0, 0.25);
        }
    }

    .ant-pagination-jump-next,
    .ant-pagination-jump-prev {
        :hover {
            .ant-pagination-item-link-icon {
                color: ${colors.violet[500]};
            }
        }
    }
`;
