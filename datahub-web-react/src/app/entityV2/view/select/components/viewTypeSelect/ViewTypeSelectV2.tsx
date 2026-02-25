import { Icon, Tooltip, borders } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';

import { ViewTypeSelectProps } from '@app/entityV2/view/select/components/viewTypeSelect/types';

const Wrapper = styled.div<{ $bordered?: boolean }>`
    display: flex;
    gap: 2px;
    align-items: center;
    background: ${(props) => props.theme.colors.bg};
    padding: 2px;
    border-radius: 16px;
    ${(props) => props.$bordered && `border: ${borders['1px']} ${props.theme.colors.border};`}
`;

const IconWrapper = styled.div<{ $active?: boolean }>`
    flex-shrink: 0;
    padding: 2px;
    background: ${(props) => (props.$active ? props.theme.colors.textBrand : props.theme.colors.bg)};
    color: ${(props) => (props.$active ? props.theme.colors.textOnFillBrand : 'inherit')};
    border-radius: 100%;
    cursor: pointer;
    width: 20px;
    height: 20px;
`;

const ICON_INACTIVE_COLOR: FontColorOptions = 'gray';
const ICON_INACTIVE_COLOR_LEVEL: FontColorLevelOptions = 1800;

const ACTIVE_ICON_PROPS = {
    color: 'inherit' as FontColorOptions,
};

const INACTIVE_ICON_PROPS = {
    color: ICON_INACTIVE_COLOR,
    colorLevel: ICON_INACTIVE_COLOR_LEVEL,
};

export default function ViewTypeSelectV2({ publicViews, privateViews, onTypeSelect, bordered }: ViewTypeSelectProps) {
    const selectedOption = useMemo(() => {
        if (publicViews && privateViews) return 'all';
        if (!publicViews && privateViews) return 'private';
        if (publicViews && !privateViews) return 'public';
        return null;
    }, [publicViews, privateViews]);

    return (
        <Wrapper $bordered={bordered} data-testid="views-type-select">
            <Tooltip placement="bottom" showArrow title="All">
                <IconWrapper onClick={() => onTypeSelect('all')} $active={selectedOption === 'all'}>
                    <Icon
                        icon="SquaresFour"
                        source="phosphor"
                        size="lg"
                        {...(selectedOption === 'all' ? ACTIVE_ICON_PROPS : INACTIVE_ICON_PROPS)}
                    />
                </IconWrapper>
            </Tooltip>

            <Tooltip placement="bottom" showArrow title="Private">
                <IconWrapper onClick={() => onTypeSelect('private')} $active={selectedOption === 'private'}>
                    <Icon
                        icon="Lock"
                        source="phosphor"
                        size="lg"
                        {...(selectedOption === 'private' ? ACTIVE_ICON_PROPS : INACTIVE_ICON_PROPS)}
                    />
                </IconWrapper>
            </Tooltip>

            <Tooltip placement="bottom" showArrow title="Public">
                <IconWrapper onClick={() => onTypeSelect('public')} $active={selectedOption === 'public'}>
                    <Icon
                        icon="GlobeHemisphereWest"
                        source="phosphor"
                        weight="fill"
                        size="lg"
                        {...(selectedOption === 'public' ? ACTIVE_ICON_PROPS : INACTIVE_ICON_PROPS)}
                    />
                </IconWrapper>
            </Tooltip>
        </Wrapper>
    );
}
