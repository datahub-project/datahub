import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';

import { RotatingButton } from '@app/shared/components';

export function RotatingTriangle({
    isOpen,
    onClick,
    testId,
}: {
    isOpen: boolean;
    onClick?: () => void;
    testId?: string;
}) {
    return (
        <RotatingButton
            ghost
            size="small"
            type="ghost"
            deg={isOpen ? 90 : 0}
            icon={<CaretRight style={{ color: 'black' }} />}
            onClick={onClick}
            data-testid={testId}
        />
    );
}
