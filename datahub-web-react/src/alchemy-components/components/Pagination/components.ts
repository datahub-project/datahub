import styled from 'styled-components';

import { spacing } from '@src/alchemy-components/theme';

export const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    margin: ${spacing.md};
    color: ${(props) => props.theme.colors.textTertiary};

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
            color: ${(props) => props.theme.colors.textTertiary};

            :hover {
                color: ${({ theme }) => theme.styles?.['primary-color']};
            }
        }
    }

    .ant-pagination-item-active > a {
        background: ${(props) => props.theme.colors.bgSurfaceBrand};
        color: ${({ theme }) => theme.styles?.['primary-color']};
        font-weight: 700;
    }

    .ant-pagination-item-link {
        display: flex;
        align-items: center;
        justify-content: center;
    }

    button {
        color: ${(props) => props.theme.colors.textTertiary};
        border: 1px solid ${(props) => props.theme.colors.border};
    }

    .ant-pagination-options {
        span,
        .ant-select-item {
            color: ${(props) => props.theme.colors.textTertiary};
        }
    }

    .ant-pagination-options-size-changer {
        .ant-select-selector {
            border: 1px solid ${(props) => props.theme.colors.border};
        }

        &:hover:not(.ant-select-disabled),
        &.ant-select-focused:not(.ant-select-disabled) {
            .ant-select-selector {
                border: 1px solid ${(props) => props.theme.colors.border};
                box-shadow: ${(props) => props.theme.colors.shadowXs};

                :hover {
                    color: ${({ theme }) => theme.styles?.['primary-color']};
                }
            }
        }
    }

    .ant-pagination-next,
    .ant-pagination-prev {
        :hover {
            box-shadow: ${(props) => props.theme.colors.shadowXs};

            button {
                color: ${({ theme }) => theme.styles?.['primary-color']};
            }
        }
    }

    .ant-pagination-disabled:hover {
        box-shadow: none;
        button {
            color: ${(props) => props.theme.colors.textDisabled};
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
