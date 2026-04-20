import React from 'react';
import styled from 'styled-components';

const Pill = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 10px;
    border: 1px solid;
    font-size: 12px;
    font-weight: 400;

    padding: 0 8px;
`;

const PrimaryKeyPill = styled(Pill)`
    color: ${(props) => props.theme.colors.textBrand} !important;
    border-color: ${(props) => props.theme.colors.borderBrand};
`;

const ForeignKeyPill = styled(Pill)`
    color: ${(props) => props.theme.colors.textSuccess} !important;
    border-color: ${(props) => props.theme.colors.borderSuccess};
`;

const NullablePill = styled(Pill)`
    color: ${(props) => props.theme.colors.textTertiary} !important;
    border-color: ${(props) => props.theme.colors.border};
`;

export function PrimaryKeyLabel() {
    return <PrimaryKeyPill>Primary Key</PrimaryKeyPill>;
}

export function ForeignKeyLabel() {
    return <ForeignKeyPill>Foreign Key</ForeignKeyPill>;
}

export function PartitioningKeyLabel() {
    return <PrimaryKeyPill>Partition Key</PrimaryKeyPill>;
}

export default function NullableLabel() {
    return <NullablePill>Nullable</NullablePill>;
}
