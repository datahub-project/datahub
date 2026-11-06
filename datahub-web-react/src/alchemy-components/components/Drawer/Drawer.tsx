import { ArrowLeft } from '@phosphor-icons/react/dist/csr/ArrowLeft';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';

import { Button } from '@components/components/Button';
import { StyledDrawer, TitleContainer, TitleLeftContainer } from '@components/components/Drawer/components';
import { maskTransparentStyle } from '@components/components/Drawer/constants';
import { drawerDefault } from '@components/components/Drawer/defaults';
import { DrawerProps } from '@components/components/Drawer/types';
import { Text } from '@components/components/Text';

type DefaultTitleBarProps = Pick<DrawerProps, 'title' | 'onBack' | 'onClose' | 'closable' | 'dataTestId'>;

const DefaultTitleBar = ({ title, onBack, onClose, closable, dataTestId }: DefaultTitleBarProps) => (
    <TitleContainer data-testid={dataTestId ? `${dataTestId}-drawer-title-container` : 'drawer-title-container'}>
        <TitleLeftContainer>
            {onBack && (
                <Button
                    color="gray"
                    icon={{ icon: ArrowLeft }}
                    iconPosition="left"
                    isCircle
                    onClick={() => onBack?.()}
                    size="xl"
                    variant="text"
                />
            )}
            <Text weight="bold" size="xl" data-testid="drawer-title">
                {title}
            </Text>
        </TitleLeftContainer>
        {closable && (
            <Button
                color="gray"
                icon={{ icon: X }}
                iconPosition="left"
                isCircle
                onClick={() => onClose?.()}
                size="xl"
                variant="text"
                data-testid="drawer-close-button"
            />
        )}
    </TitleContainer>
);

export const Drawer = ({
    title,
    header,
    showHeader = drawerDefault.showHeader,
    footer,
    children,
    open,
    onClose,
    onBack,
    width = drawerDefault.width,
    minWidth,
    maxWidth,
    closable = drawerDefault.closable,
    maskTransparent = drawerDefault.maskTransparent,
    mask = drawerDefault.mask,
    maskClosable = drawerDefault.maskClosable,
    placement = drawerDefault.placement,
    getContainer,
    push,
    autoFocus,
    bodyPadding = drawerDefault.bodyPadding,
    className,
    dataTestId,
}: React.PropsWithChildren<DrawerProps>) => {
    const hasCustomHeader = showHeader !== false && header !== undefined;

    let headerNode: React.ReactNode;
    if (showHeader === false) {
        headerNode = undefined;
    } else if (hasCustomHeader) {
        headerNode = header;
    } else {
        headerNode = (
            <DefaultTitleBar
                title={title}
                onBack={onBack}
                onClose={onClose}
                closable={closable}
                dataTestId={dataTestId}
            />
        );
    }

    const contentWrapperStyle = minWidth !== undefined || maxWidth !== undefined ? { minWidth, maxWidth } : undefined;

    return (
        <StyledDrawer
            className={className}
            onClose={onClose}
            destroyOnClose
            title={headerNode}
            footer={footer}
            open={open}
            width={width}
            contentWrapperStyle={contentWrapperStyle}
            closable={false}
            mask={mask}
            maskClosable={maskClosable}
            maskStyle={maskTransparent ? maskTransparentStyle : undefined}
            placement={placement}
            getContainer={getContainer}
            push={push}
            autoFocus={autoFocus}
            $bodyPadding={bodyPadding ?? 16}
            $hasCustomHeader={hasCustomHeader}
        >
            {children}
        </StyledDrawer>
    );
};
