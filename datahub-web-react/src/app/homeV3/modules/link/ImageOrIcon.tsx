/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
