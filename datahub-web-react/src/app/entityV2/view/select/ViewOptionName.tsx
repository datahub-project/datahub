import React from 'react';
import styled from 'styled-components';
import { colors, Tooltip } from '@components';
import FilterCenterFocusOutlinedIcon from '@mui/icons-material/FilterCenterFocusOutlined';
import LockOutlinedIcon from '@mui/icons-material/LockOutlined';
import PublicIcon from '@mui/icons-material/Public';
import { GlobeHemisphereEast, Lock } from '@phosphor-icons/react';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { ANTD_GRAY, REDESIGN_COLORS, SEARCH_COLORS } from '../../shared/constants';
import { ViewOptionTooltipTitle } from './ViewOptionTooltipTitle';
import { UserDefaultViewIcon } from '../shared/UserDefaultViewIcon';
import { GlobalDefaultViewIcon } from '../shared/GlobalDefaultViewIcon';
import { ViewDropdownMenu } from '../menu/ViewDropdownMenu';
import { DataHubView } from '../../../../types.generated';
import {
    ViewContainer,
    ViewContent,
    ViewDescription,
    ViewIcon,
    ViewIconNavBarRedesign,
    ViewLabel,
} from './styledComponents';

const ICON_WIDTH = 30;

const IconPlaceholder = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    width: ${ICON_WIDTH}px;
    display: flex;
    align-items: center;
    justify-content: end;
    position: absolute;
    right: 5px;
    top: ${(props) => (props.$isShowNavBarRedesign ? '0px' : '-5px')};
    gap: 0.2rem;
`;

const ViewType = styled.span`
    position: absolute;
    bottom: 0px;
    right: 0px;
    background-color: ${ANTD_GRAY[1]};
    color: ${REDESIGN_COLORS.BLACK};
    display: flex;
    align-items: center;
    border-radius: 8px 1px;
    padding: 5px 4px;
`;

const DefaultViewIconContainer = styled.div<{ selected?: boolean; $isShowNavBarRedesign?: boolean }>`
    border: 1px solid
        ${(props) => {
            if (props.$isShowNavBarRedesign) {
                return props.selected ? colors.violet[500] : 'transparent';
            }
            return props.selected ? SEARCH_COLORS.TITLE_PURPLE : REDESIGN_COLORS.BACKGROUND_OVERLAY_BLACK;
        }};
    border-radius: 100%;
`;

const ViewDropdownMenuContainer = styled.div`
    display: flex;
    justify-content: end;
    align-item: center;
`;

const PublicIconStyle = styled(PublicIcon)`
    font-size: 12px !important;
`;

const LockOutlinedIconStyle = styled(LockOutlinedIcon)`
    font-size: 12px !important;
`;

const FilterCenterFocusOutlinedIconStyle = styled(FilterCenterFocusOutlinedIcon)`
    font-size: 18px !important;
`;

type Props = {
    name: string;
    type: string;
    isGlobalDefault: boolean;
    isUserDefault: boolean;
    description?: string | null;
    view: DataHubView;
    visible?: boolean;
    isOwnedByUser?: boolean;
    selected?: boolean;
    // Custom Action Handlers - useful if you do NOT want the Menu to handle Modal rendering.
    onClickEdit?: () => void;
    onClickPreview?: () => void;
    selectView: () => void;
};

const ViewIconContainerNavBarRedesign = styled.div`
    display: flex;
    position: relative;
`;

export const ViewOptionName = ({
    name,
    description,
    type,
    isGlobalDefault,
    isUserDefault,
    view,
    visible,
    isOwnedByUser,
    selected,
    onClickEdit,
    onClickPreview,
    selectView,
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const renderViewIcon = () => {
        if (isShowNavBarRedesign) {
            return (
                <ViewIconContainerNavBarRedesign>
                    {(isUserDefault || isGlobalDefault) && (
                        <IconPlaceholder $isShowNavBarRedesign={isShowNavBarRedesign}>
                            {isGlobalDefault && (
                                <DefaultViewIconContainer
                                    selected={selected}
                                    $isShowNavBarRedesign={isShowNavBarRedesign}
                                >
                                    <GlobalDefaultViewIcon
                                        title="Your organization's default View."
                                        color={colors.gray[200]}
                                        size={5}
                                    />
                                </DefaultViewIconContainer>
                            )}
                            {isUserDefault && (
                                <DefaultViewIconContainer
                                    selected={selected}
                                    $isShowNavBarRedesign={isShowNavBarRedesign}
                                >
                                    <UserDefaultViewIcon
                                        title="Your default View."
                                        color={colors.violet[500]}
                                        size={5}
                                    />
                                </DefaultViewIconContainer>
                            )}
                        </IconPlaceholder>
                    )}
                    <Tooltip placement="bottom" showArrow title={type === 'GLOBAL' ? 'Public' : 'Private'}>
                        <ViewIconNavBarRedesign $selected={selected}>
                            {type === 'GLOBAL' && <GlobeHemisphereEast size={22} />}
                            {type === 'PERSONAL' && <Lock size={22} />}
                        </ViewIconNavBarRedesign>
                    </Tooltip>
                </ViewIconContainerNavBarRedesign>
            );
        }

        return (
            <ViewIcon className="create-view-icon" $selected={selected}>
                <FilterCenterFocusOutlinedIconStyle />
                {(isUserDefault || isGlobalDefault) && (
                    <IconPlaceholder>
                        {isGlobalDefault && (
                            <DefaultViewIconContainer selected={selected}>
                                <GlobalDefaultViewIcon title="Your organization's default View." size={10} />
                            </DefaultViewIconContainer>
                        )}
                        {isUserDefault && (
                            <DefaultViewIconContainer selected={selected}>
                                <UserDefaultViewIcon
                                    title="Your default View."
                                    color={REDESIGN_COLORS.TERTIARY_GREEN}
                                    size={10}
                                />
                            </DefaultViewIconContainer>
                        )}
                    </IconPlaceholder>
                )}
                <Tooltip placement="bottom" showArrow title={type === 'GLOBAL' ? 'Public' : 'Private'}>
                    <ViewType>
                        {type === 'GLOBAL' && <PublicIconStyle />}
                        {type === 'PERSONAL' && <LockOutlinedIconStyle />}
                    </ViewType>
                </Tooltip>
            </ViewIcon>
        );
    };

    return (
        <ViewContainer role="row" $selected={selected} $isShowNavBarRedesign={isShowNavBarRedesign}>
            {renderViewIcon()}
            <Tooltip
                placement="bottom"
                showArrow
                title={<ViewOptionTooltipTitle name={name} description={description} />}
            >
                <ViewContent $isShowNavBarRedesign={isShowNavBarRedesign}>
                    <ViewLabel $isShowNavBarRedesign={isShowNavBarRedesign}>{name}</ViewLabel>
                    <ViewDescription $isShowNavBarRedesign={isShowNavBarRedesign}>{description}</ViewDescription>
                </ViewContent>
            </Tooltip>
            <ViewDropdownMenuContainer>
                <ViewDropdownMenu
                    view={view}
                    isOwnedByUser={isOwnedByUser}
                    visible={visible}
                    onClickEdit={onClickEdit}
                    onClickPreview={onClickPreview}
                    selectView={selectView}
                />
            </ViewDropdownMenuContainer>
        </ViewContainer>
    );
};
