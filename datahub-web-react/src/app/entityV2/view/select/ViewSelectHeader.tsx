import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import SearchBar from '@app/entityV2/view/select/components/SearchBar';
import ViewTypeSelect from '@app/entityV2/view/select/components/viewTypeSelect/ViewTypeSelect';
import { colors } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const ViewHeader = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    justify-content: space-between;
    width: 100%;
    align-items: center;
    .select-container {
        display: flex;
        gap: 1rem;
        align-items: center;
        .select-view-icon {
            color: ${(props) => (props.$isShowNavBarRedesign ? colors.gray[1800] : REDESIGN_COLORS.BLACK)};
            display: flex;
            gap: 0.5rem;
            background: ${(props) => (props.$isShowNavBarRedesign ? colors.white : ANTD_GRAY[1])};
            border-radius: 30px;
            padding: ${(props) => (props.$isShowNavBarRedesign ? '4px' : '2px')};
            > div {
                padding: ${(props) => (props.$isShowNavBarRedesign ? '3px' : '5px 4px')};
                display: flex;
                align-item: center;
                border-radius: 100px;
                cursor: pointer;
                &.active {
                    background: ${(props) => props.theme.styles['primary-color']};
                    color: ${ANTD_GRAY[1]};
                }
            }
        }
        .select-view-label {
            font-size: 14px;
            font-weight: 700;
        }
    }
    .search-manage-container {
        display: flex;
        gap: 1rem;
        align-items: center;
        .manage {
            color: ${(props) => (props.$isShowNavBarRedesign ? colors.gray[1700] : REDESIGN_COLORS.VIEW_PURPLE)};
            font-size: 12px;
            font-weight: 700;
            cursor: pointer;
        }
    }
`;

type Props = {
    privateView: boolean;
    publicView: boolean;
    onClickViewTypeFilter: (type: string) => void;
    onClickManageViews: () => void;
    onChangeSearch: (text: any) => void;
};

export const ViewSelectHeader = ({
    publicView,
    privateView,
    onClickViewTypeFilter,
    onChangeSearch,
    onClickManageViews,
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <ViewHeader $isShowNavBarRedesign={isShowNavBarRedesign}>
            <ViewTypeSelect
                publicViews={publicView}
                privateViews={privateView}
                onTypeSelect={onClickViewTypeFilter}
                showV2={isShowNavBarRedesign}
            />
            <SearchBar onChangeSearch={onChangeSearch} onClickManageViews={onClickManageViews} minWidth="431px" />
        </ViewHeader>
    );
};
