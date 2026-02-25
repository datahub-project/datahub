import React from 'react';

import { Button } from '@components/components/Button';
import { StyledDrawer, TitleContainer, TitleLeftContainer } from '@components/components/Drawer/components';
import { maskTransparentStyle } from '@components/components/Drawer/constants';
import { drawerDefault } from '@components/components/Drawer/defaults';
import { DrawerProps } from '@components/components/Drawer/types';
import { Text } from '@components/components/Text';

export const Drawer = ({
    title,
    children,
    open,
    onClose,
    onBack,
    width = drawerDefault.width,
    closable = drawerDefault.closable,
    maskTransparent = drawerDefault.maskTransparent,
    dataTestId,
}: React.PropsWithChildren<DrawerProps>) => {
    return (
        <StyledDrawer
            onClose={onClose}
            destroyOnClose
            title={
                <TitleContainer
                    data-testid={dataTestId ? `${dataTestId}-drawer-title-container` : 'drawer-title-container'}
                >
                    <TitleLeftContainer>
                        {onBack && (
                            <Button
                                color="gray"
                                icon={{ icon: 'ArrowBack', source: 'material' }}
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
                            icon={{ icon: 'Close', source: 'material' }}
                            iconPosition="left"
                            isCircle
                            onClick={() => onClose?.()}
                            size="xl"
                            variant="text"
                            data-testid="drawer-close-button"
                        />
                    )}
                </TitleContainer>
            }
            open={open}
            width={width}
            closable={false}
            maskStyle={maskTransparent ? maskTransparentStyle : undefined}
        >
            {children}
        </StyledDrawer>
    );
};
