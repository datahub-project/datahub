import moment from 'moment/moment';
import React from 'react';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import NotesSection from '@app/entityV2/shared/notes/NotesSection';

import { Post } from '@types';

export default function SidebarNotesSection() {
    const { urn, entityData } = useEntityData();
    const refetch = useRefetch();
    const notes = entityData?.notes?.relationships
        ?.map((r) => r.entity as Post)
        ?.sort((a, b) => moment(b.lastModified.time).diff(moment(a.lastModified.time)));

    return <NotesSection urn={urn} notes={notes} refetch={() => setTimeout(() => refetch?.(), 2000)} />;
}
