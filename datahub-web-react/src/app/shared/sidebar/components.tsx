import { countries } from 'country-data-list';
import { CaretRight } from 'phosphor-react';
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
