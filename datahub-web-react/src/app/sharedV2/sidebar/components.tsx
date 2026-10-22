import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import { useTheme } from 'styled-components';

import { RotatingButton } from '@app/shared/components';

export function RotatingTriangle({
    isOpen,
    onClick,
    dataTestId,
}: {
    isOpen: boolean;
    onClick?: () => void;
    dataTestId?: string;
}) {
    const theme = useTheme();
    return (
        <RotatingButton
            ghost
            size="small"
            type="ghost"
            deg={isOpen ? 90 : 0}
            icon={<CaretRight size={24} style={{ color: theme.colors.icon }} />}
            onClick={onClick}
            data-testid={dataTestId}
        />
    );
}
