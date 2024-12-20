import React from 'react';
import { Button } from '../Button';
import { Text } from '../Text';
import { StyledDrawer, TitleContainer } from './components';
import { maskTransparentStyle } from './constants';
import { DrawerProps } from './types';

export const drawerDefault: Omit<DrawerProps, 'title'> = {
    width: 600,
    closable: true,
    maskTransparent: false,
};

export const Drawer = ({
    title,
    children,
    open,
    onClose,
    width = drawerDefault.width,
    closable = drawerDefault.closable,
    maskTransparent = drawerDefault.maskTransparent,
}: React.PropsWithChildren<DrawerProps>) => {
    return (
        <StyledDrawer
            onClose={onClose}
            destroyOnClose
            title={
                <TitleContainer>
                    <Text weight="bold" size="xl">
                        {title}
                    </Text>
                    {closable && (
                        <Button
                            color="gray"
                            icon="Close"
                            iconPosition="left"
                            isCircle
                            onClick={() => onClose?.()}
                            size="xl"
                            variant="text"
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
