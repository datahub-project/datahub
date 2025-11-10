import { Menu, Pill, Popover, Text, colors } from '@components';
import { Skeleton } from 'antd';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import usePropertyMenuItems from '@app/entityV2/summary/properties/menuProperty/usePropertyMenuItems';
import { filterCurrentItemInReplaceMenu } from '@app/entityV2/summary/properties/property/properties/utils';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

const PropertyWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    height: 52px;
`;

const Content = styled.div`
`;

const ValuesWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const PopoverValueWrapper = styled(ValuesWrapper)`
    flex-wrap: wrap;
    overflow-wrap: anywhere; // enable wrapping of long text
`;

const PopoverWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    max-width: 500px;
    width: fit-content;
`;

const PillWrapper = styled.div`
    cursor: pointer;
`;

const SquareBorderPill = styled(Pill)`
    border-radius: 4px;
`;

const Title = styled(Text)<{ $clickable?: boolean }>`
    ${(props) =>
        props.$clickable &&
        `
        cursor: pointer;
        width: fit-content;
        padding: 0 4px;
        border-radius: 4px;

        :hover {
            background: ${colors.primary[0]};
        }
    `}
`;

interface Props<T> extends PropertyComponentProps {
    values: T[];
    renderValue: (value: T) => React.ReactNode;
    renderValueInTooltip?: (value: T) => React.ReactNode;
    maxValues?: number;
    restItemsPillBorderType?: 'none' | 'rounded' | 'square';
    loading?: boolean;
}

const DEFAULT_MAX_ITEMS = 2;

export default function BaseProperty<T>({
    property,
    position,
    values,
    renderValue,
    renderValueInTooltip,
    maxValues,
    restItemsPillBorderType = 'none',
    loading,
}: Props<T>) {
    const { isTemplateEditable } = usePageTemplateContext();

    const menuItems = usePropertyMenuItems(position, property.type);

    const filteredItems = filterCurrentItemInReplaceMenu(menuItems, property);

    const valuesToShow = useMemo(() => values.slice(0, maxValues ?? DEFAULT_MAX_ITEMS), [values, maxValues]);
    const valuesToShowInPopover = useMemo(() => values.slice(maxValues ?? DEFAULT_MAX_ITEMS), [values, maxValues]);

    const renderRestOfValues = useCallback(() => {
        if (valuesToShowInPopover.length === 0) return undefined;

        const popoverContent = (
            <PopoverWrapper>
                <PopoverValueWrapper>
                    {valuesToShowInPopover.map((item) =>
                        renderValueInTooltip ? renderValueInTooltip(item) : renderValue(item),
                    )}
                </PopoverValueWrapper>
            </PopoverWrapper>
        );

        const restItemsPillText = `+${valuesToShowInPopover.length}`;

        return (
            <Popover content={popoverContent}>
                <PillWrapper>
                    {(restItemsPillBorderType === 'none' || restItemsPillBorderType === undefined) && (
                        <Text color="gray" colorLevel={1800}>
                            {restItemsPillText}
                        </Text>
                    )}
                    {restItemsPillBorderType === 'rounded' && (
                        <Pill label={restItemsPillText} variant="outline" size="sm" />
                    )}
                    {restItemsPillBorderType === 'square' && (
                        <SquareBorderPill label={restItemsPillText} variant="outline" size="sm" />
                    )}
                </PillWrapper>
            </Popover>
        );
    }, [valuesToShowInPopover, renderValueInTooltip, restItemsPillBorderType, renderValue]);

    return (
        <PropertyWrapper data-testid={`property-${property.type}`}>
            <Menu items={filteredItems} trigger={['click']} disabled={!isTemplateEditable}>
                <Title
                    weight="bold"
                    color="gray"
                    size="sm"
                    colorLevel={600}
                    $clickable={isTemplateEditable}
                    type="div"
                    data-testid="property-title"
                >
                    {property.name}
                </Title>
            </Menu>
            <Content>
                <ValuesWrapper data-testid="property-value">
                    {loading ? (
                        <Skeleton.Button active />
                    ) : (
                        <>
                            {valuesToShow.length === 0 && <Text color="gray">-</Text>}
                            {valuesToShow.map((item) => renderValue(item))}
                            {renderRestOfValues()}
                        </>
                    )}
                </ValuesWrapper>
            </Content>
        </PropertyWrapper>
    );
}
