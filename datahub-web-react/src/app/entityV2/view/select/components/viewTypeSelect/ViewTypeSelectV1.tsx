import { Tooltip } from '@components';
import GridViewIcon from '@mui/icons-material/GridView';
import LockOutlinedIcon from '@mui/icons-material/LockOutlined';
import PublicIcon from '@mui/icons-material/Public';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const VIEW_FILTER_ALL = 'all';
const VIEW_FILTER_PRIVATE = 'private';
const VIEW_FILTER_PUBLIC = 'public';

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
            color: ${(props) =>
                props.$isShowNavBarRedesign ? props.theme.colors.textTertiary : props.theme.colors.text};
            display: flex;
            gap: 0.5rem;
            background: ${(props) => (props.$isShowNavBarRedesign ? props.theme.colors.bg : props.theme.colors.bg)};
            border-radius: 30px;
            padding: ${(props) => (props.$isShowNavBarRedesign ? '4px' : '2px')};
            > div {
                padding: ${(props) => (props.$isShowNavBarRedesign ? '3px' : '5px 4px')};
                display: flex;
                align-items: center;
                border-radius: 100px;
                cursor: pointer;
                &.active {
                    background: ${(props) => props.theme.colors.buttonFillBrand};
                    color: ${(props) => props.theme.colors.bg};
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
    const { t } = useTranslation('entity.views');
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <Wrapper data-testid="views-type-select">
            <div className="select-container">
                <div className="select-view-icon">
                    <div
                        className={`${publicViews && privateViews ? 'active' : ''}`}
                        onClick={() => onTypeSelect(VIEW_FILTER_ALL)}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title={t('viewSelect.filterAll')}>
                            <GridViewIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />
                        </Tooltip>
                    </div>
                    <div
                        className={`${!publicViews && privateViews ? 'active' : ''}`}
                        onClick={() => onTypeSelect(VIEW_FILTER_PRIVATE)}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title={t('typePrivate')}>
                            <LockOutlinedIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />
                        </Tooltip>
                    </div>
                    <div
                        className={`${publicViews && !privateViews ? 'active' : ''}`}
                        onClick={() => onTypeSelect(VIEW_FILTER_PUBLIC)}
                        role="none"
                    >
                        <Tooltip placement="bottom" showArrow title={t('typePublic')}>
                            <PublicIconStyle $isShowNavBarRedesign={isShowNavBarRedesign} />
                        </Tooltip>
                    </div>
                </div>
                {!isShowNavBarRedesign && <div className="select-view-label">{t('viewSelect.selectYourView')}</div>}
            </div>
        </Wrapper>
    );
}
