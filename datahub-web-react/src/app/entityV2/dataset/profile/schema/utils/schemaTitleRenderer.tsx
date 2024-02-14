import React, { useState } from 'react';
import { Popover } from 'antd';
import { ExtendedSchemaFields } from './types';
import { ForeignKeyConstraint, SchemaMetadata } from '../../../../../../types.generated';
import { InteriorTitleContent } from '../components/InteriorTitleContent';
// import { ReactComponent as AnnouncementIcon } from '../../../../../../images/announcement.svg';

// const MAX_FIELD_PATH_LENGTH = 20;

// const LighterText = styled(Typography.Text)`
//     color: rgba(0, 0, 0, 0.45);
// `;

// ex: [type=MetadataAuditEvent].[type=union]oldSnapshot.[type=CorpUserSnapshot].[type=array]aspects.[type=union].[type=CorpUserInfo].[type=boolean]active
export default function useSchemaTitleRenderer(
    schemaMetadata: SchemaMetadata | undefined | null,
    setSelectedFkFieldPath: (params: { fieldPath: string; constraint?: ForeignKeyConstraint | null } | null) => void,
    filterText: string,
    overflowHoverFieldPath: string | null,
    isCompact?: boolean,
) {
    const [highlightedConstraint, setHighlightedConstraint] = useState<string | null>(null);

    return (fieldPath: string, record: ExtendedSchemaFields): JSX.Element => {
        return (
            <>
                <Popover
                    content={
                        <InteriorTitleContent
                            isCompact={isCompact}
                            filterText={filterText}
                            fieldPath={fieldPath}
                            record={record}
                            schemaMetadata={schemaMetadata}
                            setSelectedFkFieldPath={setSelectedFkFieldPath}
                            highlightedConstraint={highlightedConstraint}
                            setHighlightedConstraint={setHighlightedConstraint}
                        />
                    }
                    open={overflowHoverFieldPath === fieldPath}
                >
                    <InteriorTitleContent
                        isCompact={isCompact}
                        filterText={filterText}
                        fieldPath={fieldPath}
                        record={record}
                        schemaMetadata={schemaMetadata}
                        setSelectedFkFieldPath={setSelectedFkFieldPath}
                        highlightedConstraint={highlightedConstraint}
                        setHighlightedConstraint={setHighlightedConstraint}
                    />
                </Popover>
            </>
        );
    };
}
