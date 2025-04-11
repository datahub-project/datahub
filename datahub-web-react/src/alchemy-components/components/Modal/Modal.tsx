import { Button, ButtonProps, Heading, typography, Text, Icon } from '@components';
import { Modal as AntModal, ModalProps as AntModalProps } from 'antd';
import React from 'react';
import styled from 'styled-components';

const StyledModal = styled(AntModal)<{ hasChildren: boolean }>`
    font-family: ${typography.fonts.body};

    &&& .ant-modal-content {
        box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
        border-radius: 12px;
    }

    .ant-modal-header {
        //margin-bottom: 24px;
        padding: 12px 20px;
        border-radius: ${({ hasChildren }) => (hasChildren ? '12px 12px 0 0' : '12px')};
        border-bottom: ${({ hasChildren }) => (hasChildren ? `1px solid #F0F0F0` : '0')};
    }

    .ant-modal-body {
        padding: ${({ hasChildren }) => (hasChildren ? '24px 20px' : '8px 20px')};
    }

    .ant-modal-footer {
        padding: 12px 20px;
    }

    .ant-modal-close {
        top: 16px;
        right: 16px;

        .ant-modal-close-x {
            height: 24px;
            width: 24px;
        }
    }
`;

const HeaderContainer = styled.div<{ hasChildren: boolean }>`
    display: flex;
    flex-direction: column;
`;

const ButtonsContainer = styled.div`
    display: flex;
    gap: 16px;
    justify-content: end;
`;

export interface ModalButton extends ButtonProps {
    text: string;
    onClick: () => void;
}

export interface ModalProps {
    buttons: ModalButton[];
    title: string;
    subtitle?: string;
    children?: React.ReactNode;
    onCancel: () => void;
    dataTestId?: string;
}

export function Modal({
    buttons,
    title,
    subtitle,
    children,
    onCancel,
    dataTestId,
    ...props
}: ModalProps & AntModalProps) {
    return (
        <StyledModal
            open
            centered
            onCancel={onCancel}
            closeIcon={<Icon icon="X" source="phosphor" />}
            hasChildren={!!children}
            title={
                <HeaderContainer hasChildren={!!children}>
                    <Heading type="h1" color="gray" colorLevel={600} weight="bold" size="lg">
                        {title}
                    </Heading>
                    {!!subtitle && (
                        <Text type="span" color="gray" colorLevel={1700} weight="medium">
                            {subtitle}
                        </Text>
                    )}
                </HeaderContainer>
            }
            footer={
                !!buttons.length && (
                    <ButtonsContainer>
                        {buttons.map(({ text, variant, onClick, ...buttonProps }, index) => (
                            <Button
                                key={text}
                                data-testid={dataTestId && `${dataTestId}-${variant}-${index}`}
                                variant={variant}
                                onClick={onClick}
                                {...buttonProps}
                            >
                                <Text type="span" weight="bold" lineHeight="none">
                                    {text}
                                </Text>
                            </Button>
                        ))}
                    </ButtonsContainer>
                )
            }
            {...props}
        >
            {children}
        </StyledModal>
    );
}
