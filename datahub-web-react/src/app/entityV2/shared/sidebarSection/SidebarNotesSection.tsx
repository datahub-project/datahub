import dayjs from 'dayjs';
import React from 'react';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import NotesSection from '@app/entityV2/shared/notes/NotesSection';

import { Post } from '@types';

export default function SidebarNotesSection() {
    const { urn, entityData } = useEntityData();
    const refetch = useRefetch();
    const notes = entityData?.notes?.relationships
        ?.map((r) => r.entity as Post)
        ?.sort((a, b) => dayjs(b.lastModified.time).diff(dayjs(a.lastModified.time)));

    return <NotesSection urn={urn} notes={notes} refetch={() => setTimeout(() => refetch?.(), 2000)} />;
}
