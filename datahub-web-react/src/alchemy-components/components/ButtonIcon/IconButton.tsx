import { Button, ButtonProps, Icon, IconProps } from '@components';
import React from 'react';

// const StyledButton = styled`
//     display: flex;
//     align-items: center;
//     height: fit-content;
//     padding: 4px;
// `;

interface Props {
    buttonProps: ButtonProps;
    iconProps: IconProps;
}

export default function IconButton({ buttonProps, iconProps }: Props) {
    return (
        <Button variant="text" {...buttonProps}>
            <Icon {...iconProps} />
        </Button>
    );
}
