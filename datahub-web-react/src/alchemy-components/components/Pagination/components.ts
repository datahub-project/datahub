import styled from 'styled-components';

import { colors, spacing } from '@src/alchemy-components/theme';

export const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    margin: ${spacing.md};
    color: ${colors.gray[1800]};

    .ant-pagination {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 4px;

        li {
            margin-right: 0px;
        }
    }

    .ant-pagination-item {
        border: none;

        a {
            border-radius: 200px;
            color: ${colors.gray[1800]};

            :hover {
                color: ${({ theme }) => theme.styles?.['primary-color']};
            }
        }
    }

    .ant-pagination-item-active > a {
        background: ${colors.violet[0]};
        color: ${({ theme }) => theme.styles?.['primary-color']};
        font-weight: 700;
    }

    .ant-pagination-item-link {
        display: flex;
        align-items: center;
        justify-content: center;
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

                :hover {
                    color: ${({ theme }) => theme.styles?.['primary-color']};
                }
            }
        }
    }

    .ant-pagination-next,
    .ant-pagination-prev {
        :hover {
            box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);

            button {
                color: ${({ theme }) => theme.styles?.['primary-color']};
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
                color: ${({ theme }) => theme.styles?.['primary-color']};
            }
        }
    }
`;
