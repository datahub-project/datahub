import { Pill } from '@components';
import React from 'react';

interface Props {
    isPublic?: boolean;
}

export default function PublicModuleBadge({ isPublic }: Props) {
    if (!isPublic) return null;

    return <Pill label="Public" color="gray" size="xs" />;
}
