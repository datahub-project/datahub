import { Icon } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

const Image = styled.img`
    height: 24px;
    width: 24px;
    object-fit: contain;
    background-color: transparent;
`;

export default function ImageOrIcon({ imageUrl }: { imageUrl?: string | null }) {
    const [hasError, setHasError] = useState(false);

    React.useEffect(() => {
        setHasError(false);
    }, [imageUrl]);

    if (imageUrl && !hasError) {
        return <Image src={imageUrl} alt="Link image" onError={() => setHasError(true)} />;
    }

    return <Icon icon="LinkSimple" source="phosphor" size="3xl" color="gray" />;
}
