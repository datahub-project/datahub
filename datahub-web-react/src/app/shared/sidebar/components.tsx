import { countries } from 'country-data-list';
import React from 'react';
import { RightOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { RotatingButton } from '../components';

export const SidebarWrapper = styled.div<{ width: number }>`
    max-height: 100%;
    width: ${(props) => props.width}px;
    min-width: ${(props) => props.width}px;
    display: ${(props) => (props.width ? 'block' : 'none')};
    display: flex;
    flex-direction: column;
`;

export function RotatingTriangle({ isOpen, onClick }: { isOpen: boolean; onClick?: () => void }) {
    return (
        <RotatingButton
            ghost
            size="small"
            type="ghost"
            deg={isOpen ? 90 : 0}
            icon={<RightOutlined style={{ color: 'black' }} />}
            onClick={onClick}
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
