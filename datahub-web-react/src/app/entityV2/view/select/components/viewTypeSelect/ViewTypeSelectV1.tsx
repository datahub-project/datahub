import { Tooltip, colors } from '@components';
import GridViewIcon from '@mui/icons-material/GridView';
import LockOutlinedIcon from '@mui/icons-material/LockOutlined';
import PublicIcon from '@mui/icons-material/Public';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const GridViewIconStyle = styled(GridViewIcon)<{ $isShowNavBarRedesign?: boolean }>`
    font-size: ${(props) => (props.$isShowNavBarRedesign ? '14px' : '13px')} !important;
`;

const LockOutlinedIconStyle = styled(LockOutlinedIcon)<{ $isShowNavBarRedesign?: boolean }>`
    font-size: ${(props) => (props.$isShowNavBarRedesign ? '14px' : '13px')} !important;
`;

const PublicIconStyle = styled(PublicIcon)<{ $isShowNavBarRedesign?: boolean }>`
    font-size: ${(props) => (props.$isShowNavBarRedesign ? '14px' : '13px')} !important;
`;

const Wrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
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
                align-items: center;
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
`;

interface Props {
    publicViews: boolean;
    privateViews: boolean;
    onTypeSelect: (type: string) => void;
}

export default function ViewTypeSelectV1({ publicViews, privateViews, onTypeSelect }: Props) {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <Wrapper>
            <div className="select-container">
                <div className="select-view-icon">
                    <div
                        className={`${publicViews && privateViews ? 'active' : ''}`}
                        onClick={() => onTypeSelect('all')}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title="All">
                            <GridViewIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />
                        </Tooltip>
                    </div>
                    <div
                        className={`${!publicViews && privateViews ? 'active' : ''}`}
                        onClick={() => onTypeSelect('private')}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title="Private">
                            <LockOutlinedIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />
                        </Tooltip>
                    </div>
                    <div
                        className={`${publicViews && !privateViews ? 'active' : ''}`}
                        onClick={() => onTypeSelect('public')}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title="Public">
                            <PublicIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />
                        </Tooltip>
                    </div>
                </div>
                {!isShowNavBarRedesign && <div className="select-view-label">Select Your View</div>}
            </div>
        </Wrapper>
    );
}
