import { countries } from 'country-data-list';
import { CaretRight } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { RotatingButton } from '@app/shared/components';

export const SidebarWrapper = styled.div<{ width: number }>`
    max-height: 100%;
    width: ${(props) => props.width}px;
    min-width: ${(props) => props.width}px;
    display: ${(props) => (props.width ? 'block' : 'none')};
    display: flex;
    flex-direction: column;
`;

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

export function getCountryName(countryCode: string) {
    let countryName;
    const findCountryName = (code) => {
        try {
            countryName = countries[code].name;
        } catch (error) {
            countryName = null;
        }
    };

    if (countryCode === '' || countryCode === undefined || countryCode == null) {
        return null;
    }

    findCountryName(countryCode);
    return countryName;
}
