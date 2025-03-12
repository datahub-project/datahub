import React from 'react';
import { Button } from '../Button';
import { Text } from '../Text';
import { StyledDrawer, TitleContainer, TitleLeftContainer } from './components';
import { maskTransparentStyle } from './constants';
import { DrawerProps } from './types';
import { drawerDefault } from './defaults';

export const Drawer = ({
    title,
    children,
    open,
    onClose,
    onBack,
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
                    <TitleLeftContainer>
                        {onBack && (
                            <Button
                                color="gray"
                                icon="ArrowBack"
                                iconPosition="left"
                                isCircle
                                onClick={() => onBack?.()}
                                size="xl"
                                variant="text"
                            />
                        )}
                        <Text weight="bold" size="xl">
                            {title}
                        </Text>
                    </TitleLeftContainer>
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
