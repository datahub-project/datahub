import moment from 'moment/moment';
import React from 'react';
import { Post } from '../../../../types.generated';
import { useEntityData, useRefetch } from '../../../entity/shared/EntityContext';
import NotesSection from '../notes/NotesSection';

export default function SidebarNotesSection() {
    const { urn, entityData } = useEntityData();
    const refetch = useRefetch();
    const notes = entityData?.notes?.relationships
        ?.map((r) => r.entity as Post)
        ?.sort((a, b) => moment(b.lastModified.time).diff(moment(a.lastModified.time)));

    return <NotesSection urn={urn} notes={notes} refetch={() => setTimeout(() => refetch?.(), 2000)} />;
}
