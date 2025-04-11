import React from 'react';
import styled from 'styled-components';
import GridViewIcon from '@mui/icons-material/GridView';
import LockOutlinedIcon from '@mui/icons-material/LockOutlined';
import PublicIcon from '@mui/icons-material/Public';
import { Input } from 'antd';
import { Tooltip } from '@components';
import { SearchOutlined } from '@ant-design/icons';
import { colors } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { MagnifyingGlass } from '@phosphor-icons/react';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../shared/constants';

const StyledInput = styled(Input)<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) => !props.$isShowNavBarRedesign && 'max-width: 330px;'}
    background-color: ${(props) =>
        props.$isShowNavBarRedesign ? 'white' : REDESIGN_COLORS.BACKGROUND_OVERLAY_BLACK_SEARCH};
    border-radius: ${(props) => (props.$isShowNavBarRedesign ? '8px' : '7px')};

    ${(props) => !props.$isShowNavBarRedesign && 'border: unset;'}

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        min-width: 431px;
        height: 40px;
        border: 1px solid;
        border-color: ${colors.gray[100]};
        box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);

        &&:hover {
            border-color: ${colors.violet[500]};
        }

        &.ant-input-affix-wrapper-focused {
            border-color: ${colors.violet[500]};
        }
        
        & .ant-input::placeholder {
            color: ${colors.gray[1800]};
        }

        & .ant-input-prefix {
            margin-right: 8px;
            svg {
                color: ${colors.gray[1800]}
            }
        }
    `}

    & .ant-input {
        background-color: transparent;
        ${(props) => !props.$isShowNavBarRedesign && `color: ${colors.gray[1800]};`}
        ${(props) => props.$isShowNavBarRedesign && 'font-size: 14px;'}
    }
`;

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
            color: ${(props) => (props.$isShowNavBarRedesign ? colors.gray[1700] : REDESIGN_COLORS.VIEW_PURPLE)};
            font-size: 12px;
            font-weight: 700;
            cursor: pointer;
        }
    }
`;

const GridViewIconStyle = styled(GridViewIcon)<{ $isShowNavBarRedesign?: boolean }>`
    font-size: ${(props) => (props.$isShowNavBarRedesign ? '14px' : '13px')} !important;
`;

const LockOutlinedIconStyle = styled(LockOutlinedIcon)<{ $isShowNavBarRedesign?: boolean }>`
    font-size: ${(props) => (props.$isShowNavBarRedesign ? '14px' : '13px')} !important;
`;

const PublicIconStyle = styled(PublicIcon)<{ $isShowNavBarRedesign?: boolean }>`
    font-size: ${(props) => (props.$isShowNavBarRedesign ? '14px' : '13px')} !important;
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
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <ViewHeader $isShowNavBarRedesign={isShowNavBarRedesign}>
            <div className="select-container">
                <div className="select-view-icon">
                    <div
                        className={`${publicView && privateView ? 'active' : ''}`}
                        onClick={() => onClickViewTypeFilter('all')}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title="All">
                            <GridViewIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />
                        </Tooltip>
                    </div>
                    <div
                        className={`${!publicView && privateView ? 'active' : ''}`}
                        onClick={() => onClickViewTypeFilter('private')}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title="Private">
                            <LockOutlinedIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />
                        </Tooltip>
                    </div>
                    <div
                        className={`${publicView && !privateView ? 'active' : ''}`}
                        onClick={() => onClickViewTypeFilter('public')}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title="Public">
                            <PublicIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />
                        </Tooltip>
                    </div>
                </div>
                {!isShowNavBarRedesign && <div className="select-view-label">Select Your View</div>}
            </div>
            <div className="search-manage-container">
                <div>
                    <StyledInput
                        className="style-input-container"
                        placeholder={isShowNavBarRedesign ? 'Search views...' : 'Search'}
                        onChange={onChangeSearch}
                        allowClear
                        prefix={isShowNavBarRedesign ? <MagnifyingGlass size={20} /> : <SearchOutlinedStyle />}
                        data-testid="search-overlay-input"
                        $isShowNavBarRedesign={isShowNavBarRedesign}
                    />
                </div>
                <div className="manage" onClick={() => onClickManageViews()} role="none">
                    Manage all
                </div>
            </div>
        </ViewHeader>
    );
};
