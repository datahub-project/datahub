import { Icon, Tooltip, colors } from '@components';
import FilterCenterFocusOutlinedIcon from '@mui/icons-material/FilterCenterFocusOutlined';
import { GlobeHemisphereEast, Lock } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { ViewDropdownMenu } from '@app/entityV2/view/menu/ViewDropdownMenu';
import { ViewOptionTooltipTitle } from '@app/entityV2/view/select/ViewOptionTooltipTitle';
import {
    CardViewLabel,
    ViewContainer,
    ViewContent,
    ViewDescription,
    ViewIcon,
    ViewIconNavBarRedesign,
} from '@app/entityV2/view/select/styledComponents';
import { GlobalDefaultViewIcon } from '@app/entityV2/view/shared/GlobalDefaultViewIcon';
import { UserDefaultViewIcon } from '@app/entityV2/view/shared/UserDefaultViewIcon';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { useCustomTheme } from '@src/customThemeContext';

import { DataHubView } from '@types';

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
                return props.selected ? props.theme.styles['primary-color'] : 'transparent';
            }
            return props.selected ? props.theme.styles['primary-color'] : REDESIGN_COLORS.BACKGROUND_OVERLAY_BLACK;
        }};
    border-radius: 100%;
`;

const ViewDropdownMenuContainer = styled.div`
    display: flex;
    justify-content: end;
    align-items: center;
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
    fixedWidth?: boolean;
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
    fixedWidth,
    onClickEdit,
    onClickPreview,
    selectView,
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { theme } = useCustomTheme();

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
                                        color={theme?.styles['primary-color']}
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
                        {type === 'GLOBAL' && <Icon icon="GlobeHemisphereEast" source="phosphor" size="lg" />}
                        {type === 'PERSONAL' && <Icon icon="Lock" source="phosphor" size="lg" />}
                    </ViewType>
                </Tooltip>
            </ViewIcon>
        );
    };

    return (
        <ViewContainer
            role="row"
            $selected={selected}
            $isShowNavBarRedesign={isShowNavBarRedesign}
            $fixedWidth={fixedWidth}
        >
            {renderViewIcon()}
            <Tooltip
                placement="bottom"
                showArrow
                title={<ViewOptionTooltipTitle name={name} description={description} />}
            >
                <ViewContent $isShowNavBarRedesign={isShowNavBarRedesign} $fixedWidth={fixedWidth}>
                    <CardViewLabel $isShowNavBarRedesign={isShowNavBarRedesign}>{name}</CardViewLabel>
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
