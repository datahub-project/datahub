import React, { forwardRef, useEffect, useRef, useState } from 'react';

import { Icon } from '@components/components/Icon';
import { Tooltip, TooltipPlacement } from '@components/components/Tooltip';
import { IconContainer, Label, TabButton } from '@components/components/VerticalTabs/components';
import { VerticalTabItem } from '@components/components/VerticalTabs/types';

type Props = {
    item: VerticalTabItem;
    isActive: boolean;
    isFocusable: boolean;
    gradientId: string;
    tooltipPlacement: TooltipPlacement;
    id?: string;
    onClick: (key: string) => void;
    onKeyDown: (event: React.KeyboardEvent<HTMLButtonElement>) => void;
};

function getAriaLabel(item: VerticalTabItem): string | undefined {
    if (item.ariaLabel) return item.ariaLabel;
    if (item.label) return item.label;
    return typeof item.tooltip === 'string' ? item.tooltip : undefined;
}

export const VerticalTab = forwardRef<HTMLButtonElement, Props>(
    ({ item, isActive, isFocusable, gradientId, tooltipPlacement, id, onClick, onKeyDown }, ref) => {
        const labelRef = useRef<HTMLSpanElement>(null);
        const [isLabelTruncated, setIsLabelTruncated] = useState(false);

        useEffect(() => {
            const element = labelRef.current;
            if (element) setIsLabelTruncated(element.scrollWidth > element.clientWidth);
        }, [item.label]);

        const IconComponent = isActive && item.selectedIcon ? item.selectedIcon : item.icon;
        const tooltip = item.tooltip ?? (isLabelTruncated ? item.label : undefined);

        return (
            <Tooltip title={tooltip} placement={tooltipPlacement}>
                <TabButton
                    ref={ref}
                    id={id}
                    type="button"
                    role="tab"
                    aria-selected={isActive}
                    aria-label={getAriaLabel(item)}
                    tabIndex={isFocusable ? 0 : -1}
                    disabled={item.disabled}
                    data-testid={item.dataTestId}
                    $active={isActive}
                    $gradientId={gradientId}
                    onClick={() => onClick(item.key)}
                    onKeyDown={onKeyDown}
                >
                    <IconContainer>
                        <Icon icon={IconComponent} size="2xl" weight={isActive ? 'fill' : 'regular'} />
                    </IconContainer>
                    {item.label && (
                        <Label ref={labelRef} $active={isActive}>
                            {item.label}
                        </Label>
                    )}
                </TabButton>
            </Tooltip>
        );
    },
);

VerticalTab.displayName = 'VerticalTab';
