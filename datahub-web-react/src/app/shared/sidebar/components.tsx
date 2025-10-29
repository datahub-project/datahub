import { countries } from 'country-data-list';
import { CaretRight } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

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
