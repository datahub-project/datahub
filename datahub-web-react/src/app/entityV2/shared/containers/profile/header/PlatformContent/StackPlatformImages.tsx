import React from 'react';
import styled from 'styled-components';
import PlatformIcon from '../../../../../../sharedV2/icons/PlatformIcon';
import { DataPlatform } from '../../../../../../../types.generated';

const Container = styled.div`
    display: flex;
    align-items: center;
    position: relative;
`;

const secondIconStyles = {
    marginLeft: '-16px',
    zIndex: 0,
    borderRadius: '16px',
    border: '1px solid #FFF',
    padding: '10px',
};

const firstIconStyles = {
    zIndex: 1,
    borderRadius: '16px',
    border: '1px solid #FFF',
    padding: '10px',
};

interface Props {
    platforms: DataPlatform[];
}

const StackImages = ({ platforms }: Props) => {
    const uniquePlatforms = platforms.reduce<DataPlatform[]>((acc, current) => {
        if (!acc.find((platform) => platform.urn === current.urn)) {
            acc.push(current);
        }
        return acc;
    }, []);

    return (
        <Container>
            {uniquePlatforms.slice(0, 2).map((platform, index) => (
                <>
                    {index === 1 ? (
                        <PlatformIcon platform={platform} size={28} styles={secondIconStyles} />
                    ) : (
                        <PlatformIcon platform={platform} size={28} styles={firstIconStyles} />
                    )}
                </>
            ))}
        </Container>
    );
};

export default StackImages;
