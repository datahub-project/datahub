import React from 'react';
import styled from 'styled-components';
import GridViewIcon from '@mui/icons-material/GridView';
import LockOutlinedIcon from '@mui/icons-material/LockOutlined';
import PublicIcon from '@mui/icons-material/Public';
import { Input } from 'antd';
import { Tooltip } from '@components';
import { SearchOutlined } from '@ant-design/icons';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../shared/constants';

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 300px;
    background-color: ${REDESIGN_COLORS.BACKGROUND_OVERLAY_BLACK_SEARCH};
    border-radius: 7px;
    border: unset;
    & .ant-input {
        background-color: transparent;
        color: ${ANTD_GRAY[5]};
    }
`;

const ViewHeader = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
    align-items: center;
    .select-container {
        display: flex;
        gap: 1rem;
        align-items: center;
        .select-view-icon {
            color: ${REDESIGN_COLORS.BLACK};
            display: flex;
            gap: 0.5rem;
            background: ${ANTD_GRAY[1]};
            border-radius: 30px;
            padding: 2px;
            > div {
                padding: 5px 4px;
                display: flex;
                align-item: center;
                border-radius: 100px;
                cursor: pointer;
                &.active {
                    background: #533fd1;
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
            color: ${REDESIGN_COLORS.VIEW_PURPLE};
            font-size: 12px;
            font-weight: 700;
            cursor: pointer;
        }
    }
`;

const GridViewIconStyle = styled(GridViewIcon)`
    font-size: 13px !important;
`;

const LockOutlinedIconStyle = styled(LockOutlinedIcon)`
    font-size: 13px !important;
`;

const PublicIconStyle = styled(PublicIcon)`
    font-size: 13px !important;
`;

const SearchOutlinedStyle = styled(SearchOutlined)`
    color: ${ANTD_GRAY[5]};
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
    return (
        <ViewHeader>
            <div className="select-container">
                <div className="select-view-icon">
                    <div
                        className={`${publicView && privateView ? 'active' : ''}`}
                        onClick={() => onClickViewTypeFilter('all')}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title="All">
                            <GridViewIconStyle />
                        </Tooltip>
                    </div>
                    <div
                        className={`${!publicView && privateView ? 'active' : ''}`}
                        onClick={() => onClickViewTypeFilter('private')}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title="Private">
                            <LockOutlinedIconStyle />
                        </Tooltip>
                    </div>
                    <div
                        className={`${publicView && !privateView ? 'active' : ''}`}
                        onClick={() => onClickViewTypeFilter('public')}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title="Public">
                            <PublicIconStyle />
                        </Tooltip>
                    </div>
                </div>
                <div className="select-view-label">Select Your View</div>
            </div>
            <div className="search-manage-container">
                <div>
                    <StyledInput
                        className="style-input-container"
                        placeholder="Search"
                        onChange={onChangeSearch}
                        allowClear
                        prefix={<SearchOutlinedStyle />}
                        data-testid="search-overlay-input"
                    />
                </div>
                <div className="manage" onClick={() => onClickManageViews()} role="none">
                    Manage All
                </div>
            </div>
        </ViewHeader>
    );
};
