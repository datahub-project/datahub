import styled from 'styled-components';

import { spacing } from '@src/alchemy-components/theme';

export const PaginationContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    gap: ${spacing.sm};
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

    /* The page-number pill is drawn on the inner anchor, so the list item itself stays
       transparent — otherwise antd's square item background shows behind the circle. */
    .ant-pagination-item,
    .ant-pagination-item-active {
        border: none;
        background: transparent;
        box-shadow: none;

        a {
            border-radius: 200px;
            color: ${(props) => props.theme.colors.textTertiary};

            :hover {
                color: ${(props) => props.theme.colors.textBrand};
            }
        }
    }

    .ant-pagination-item-active > a {
        background: ${(props) => props.theme.colors.bgSurfaceBrand};
        color: ${(props) => props.theme.colors.textBrand};
        font-weight: 700;
    }

    .ant-pagination-item-link {
        display: flex;
        align-items: center;
        justify-content: center;
    }

    /* Matches the prev/next selectors in GlobalThemeStyles so the borderless treatment
       wins over the legacy antd pagination theming applied to the same markup. */
    .ant-pagination-prev .ant-pagination-item-link,
    .ant-pagination-next .ant-pagination-item-link {
        color: ${(props) => props.theme.colors.icon};
        border: none;
        background: ${(props) => props.theme.colors.bg};
        box-shadow: ${(props) => props.theme.colors.shadowXs};
    }

    .ant-pagination-prev:hover .ant-pagination-item-link,
    .ant-pagination-next:hover .ant-pagination-item-link {
        box-shadow: ${(props) => props.theme.colors.shadowSm};
        color: ${(props) => props.theme.colors.iconHover};
    }

    .ant-pagination-disabled .ant-pagination-item-link,
    .ant-pagination-disabled:hover .ant-pagination-item-link {
        background: ${(props) => props.theme.colors.bgSurfaceDisabled};
        box-shadow: none;
        color: ${(props) => props.theme.colors.iconDisabled};
    }

    .ant-pagination-jump-next,
    .ant-pagination-jump-prev {
        .ant-pagination-item-ellipsis {
            color: ${(props) => props.theme.colors.icon};
        }

        :hover {
            .ant-pagination-item-link-icon {
                color: ${(props) => props.theme.colors.iconHover};
            }
        }
    }
`;
