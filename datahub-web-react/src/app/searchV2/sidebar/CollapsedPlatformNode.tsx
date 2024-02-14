import React from 'react';
import styled from 'styled-components';
import { DataPlatform } from "../../../types.generated";

export const PlatformButton = styled.img<{ size?: number }>`
    max-height: ${(props) => (props.size ? props.size : 12)}px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    margin-top: 12px;
    margin-bottom: 12px;
    :hover {
        cursor: pointer;
    }
`;

type Props = {
    platform: DataPlatform; 
    onClick: () => void;
}

const CollapsedPlatformNode = ({ platform, onClick }: Props) => {
    const logoUrl = (platform as DataPlatform)?.properties?.logoUrl;
    if (!logoUrl) {
        return null;
    }
    return (
        <PlatformButton src={logoUrl as any} size={24} onClick={onClick}/>

    );
}

export default CollapsedPlatformNode; 