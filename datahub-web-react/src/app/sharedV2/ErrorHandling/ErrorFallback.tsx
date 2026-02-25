import { Button, Text } from '@components';
import React from 'react';
import { useHistory } from 'react-router';
import styled, { css } from 'styled-components';

type ErrorFallbackProps = {
    variant?: ErrorVariant;
    actionMessage?: string;
};
const DEFAULT_MESSAGE = 'Our team has been notified of this unexpected error and is working on a resolution.';
export type ErrorVariant = 'route' | 'tab' | 'sidebar';

const getVariantStyles = (variant: ErrorVariant) => {
    // only route needs the border shadows and margins
    if (variant === 'route') {
        return css`
            border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
            box-shadow: ${(props) => props.theme.styles['box-shadow-navbar-redesign']};
            margin: 5px;
        `;
    }
    return '';
};

const Container = styled.div<{ variant?: ErrorVariant }>`
    background-color: white;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    width: 100%;
    gap: 16px;

    ${(props) => getVariantStyles(props.variant || 'route')}

    svg {
        width: 160px;
        height: 160px;
    }
`;

const ButtonContainer = styled.div`
    display: flex;
    gap: 8px;
`;

const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 0 20px;
    text-align: center;
`;

const ErrorFallback: React.FC<ErrorFallbackProps> = ({ variant, actionMessage = DEFAULT_MESSAGE }) => {
    const history = useHistory();
    return (
        <Container variant={variant}>
            <svg width="200" height="200" viewBox="0 0 200 200" fill="none" xmlns="http://www.w3.org/2000/svg">
                <rect width="200" height="200" rx="100" fill="#F9FAFC" />
                <path
                    d="M110 145C110 146.978 109.414 148.911 108.315 150.556C107.216 152.2 105.654 153.482 103.827 154.239C102 154.996 99.9889 155.194 98.0491 154.808C96.1093 154.422 94.3275 153.47 92.9289 152.071C91.5304 150.673 90.578 148.891 90.1922 146.951C89.8063 145.011 90.0043 143 90.7612 141.173C91.5181 139.346 92.7998 137.784 94.4443 136.685C96.0888 135.587 98.0222 135 100 135C102.652 135 105.196 136.054 107.071 137.929C108.946 139.804 110 142.348 110 145ZM100 120C101.326 120 102.598 119.473 103.536 118.536C104.473 117.598 105 116.326 105 115V50C105 48.6739 104.473 47.4021 103.536 46.4645C102.598 45.5268 101.326 45 100 45C98.6739 45 97.4022 45.5268 96.4645 46.4645C95.5268 47.4021 95 48.6739 95 50V115C95 116.326 95.5268 117.598 96.4645 118.536C97.4022 119.473 98.6739 120 100 120Z"
                    fill="#67739E"
                />
            </svg>
            <TextContainer>
                <Text color="gray" weight="bold" size="xl">
                    Whoops!
                </Text>
                <TextContainer>
                    <Text color="gray" size="lg">
                        Something didn&apos;t go as planned.
                    </Text>
                    <Text color="gray" size="lg">
                        {actionMessage}
                    </Text>
                </TextContainer>
            </TextContainer>
            {variant !== 'sidebar' && (
                <ButtonContainer>
                    <Button size="sm" variant="outline" onClick={() => history.go(0)}>
                        Refresh
                    </Button>
                    <Button size="sm" variant="filled" onClick={() => history.push('/')}>
                        Home
                    </Button>
                </ButtonContainer>
            )}
        </Container>
    );
};

export default ErrorFallback;
