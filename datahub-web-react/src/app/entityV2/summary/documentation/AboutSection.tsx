import { Text } from '@components';
import React from 'react';

import AboutContent from '@app/entityV2/summary/documentation/AboutContent';

export default function AboutSection() {
    return (
        <>
            <Text weight="bold" color="gray" colorLevel={600}>
                About
            </Text>
            <AboutContent />
        </>
    );
}
