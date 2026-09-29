import { Icon, Tooltip, borders } from '@components';
import { GlobeHemisphereWest } from '@phosphor-icons/react/dist/csr/GlobeHemisphereWest';
import { Lock } from '@phosphor-icons/react/dist/csr/Lock';
import { SquaresFour } from '@phosphor-icons/react/dist/csr/SquaresFour';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';

import { ViewTypeSelectProps } from '@app/entityV2/view/select/components/viewTypeSelect/types';

const VIEW_FILTER_ALL = 'all';
const VIEW_FILTER_PRIVATE = 'private';
const VIEW_FILTER_PUBLIC = 'public';

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
    const { t } = useTranslation('entity.views');
    const selectedOption = useMemo(() => {
        if (publicViews && privateViews) return VIEW_FILTER_ALL;
        if (!publicViews && privateViews) return VIEW_FILTER_PRIVATE;
        if (publicViews && !privateViews) return VIEW_FILTER_PUBLIC;
        return null;
    }, [publicViews, privateViews]);

    return (
        <Wrapper $bordered={bordered} data-testid="views-type-select">
            <Tooltip placement="bottom" showArrow title={t('viewSelect.filterAll')}>
                <IconWrapper onClick={() => onTypeSelect(VIEW_FILTER_ALL)} $active={selectedOption === VIEW_FILTER_ALL}>
                    <Icon
                        icon={SquaresFour}
                        size="lg"
                        {...(selectedOption === VIEW_FILTER_ALL ? ACTIVE_ICON_PROPS : INACTIVE_ICON_PROPS)}
                    />
                </IconWrapper>
            </Tooltip>

            <Tooltip placement="bottom" showArrow title={t('typePrivate')}>
                <IconWrapper
                    onClick={() => onTypeSelect(VIEW_FILTER_PRIVATE)}
                    $active={selectedOption === VIEW_FILTER_PRIVATE}
                >
                    <Icon
                        icon={Lock}
                        size="lg"
                        {...(selectedOption === VIEW_FILTER_PRIVATE ? ACTIVE_ICON_PROPS : INACTIVE_ICON_PROPS)}
                    />
                </IconWrapper>
            </Tooltip>

            <Tooltip placement="bottom" showArrow title={t('typePublic')}>
                <IconWrapper
                    onClick={() => onTypeSelect(VIEW_FILTER_PUBLIC)}
                    $active={selectedOption === VIEW_FILTER_PUBLIC}
                >
                    <Icon
                        icon={GlobeHemisphereWest}
                        weight="fill"
                        size="lg"
                        {...(selectedOption === VIEW_FILTER_PUBLIC ? ACTIVE_ICON_PROPS : INACTIVE_ICON_PROPS)}
                    />
                </IconWrapper>
            </Tooltip>
        </Wrapper>
    );
}
