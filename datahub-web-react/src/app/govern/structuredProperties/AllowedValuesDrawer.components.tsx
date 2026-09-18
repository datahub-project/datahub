import { Button } from '@components';
import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { DotsSixVertical } from '@phosphor-icons/react/dist/csr/DotsSixVertical';
import React from 'react';
import styled from 'styled-components';

// The drawer stacks each value's inputs vertically, so the handle gets its own column and
// `align-items: center` centers it against the full height of the stacked fields. With nothing to
// reorder the column collapses entirely so the fields keep their usual alignment.
const SortableRow = styled.div<{
    $isDragging: boolean;
    $isDisabled: boolean;
    $transform?: string;
    $transition?: string;
}>`
    display: grid;
    grid-template-columns: ${({ $isDisabled }) => ($isDisabled ? '1fr' : 'auto 1fr')};
    align-items: center;
    column-gap: ${({ $isDisabled }) => ($isDisabled ? '0' : '8px')};
    border-radius: 6px;
    background-color: ${(props) => (props.$isDragging ? props.theme.colors.bgSurface : 'transparent')};
    box-shadow: ${(props) => (props.$isDragging ? props.theme.colors.shadowSm : 'none')};
    z-index: ${(props) => (props.$isDragging ? '999' : 'auto')};
    position: ${(props) => (props.$isDragging ? 'relative' : 'static')};
    transform: ${(props) => props.$transform};
    transition: ${(props) => props.$transition};
`;

const HandleCell = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
`;

const ContentCell = styled.div`
    min-width: 0;
`;

const DragButton = styled(Button)<{ $isDragging: boolean }>`
    cursor: ${(props) => (props.$isDragging ? 'grabbing' : 'grab')};
`;

type Props = {
    id: string;
    isDisabled?: boolean;
    dragLabel: string;
    children: React.ReactNode;
};

const SortableValueRow = ({ id, isDisabled, dragLabel, children }: Props) => {
    const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
        id,
        disabled: isDisabled,
    });

    return (
        <SortableRow
            ref={setNodeRef}
            {...attributes}
            $isDragging={isDragging}
            $isDisabled={!!isDisabled}
            $transform={CSS.Transform.toString(transform)}
            $transition={transition}
        >
            {!isDisabled && (
                <HandleCell>
                    <DragButton
                        {...listeners}
                        aria-label={dragLabel}
                        variant="text"
                        isCircle
                        icon={{ icon: DotsSixVertical, size: 'lg', color: 'gray' }}
                        $isDragging={isDragging}
                    />
                </HandleCell>
            )}
            <ContentCell>{children}</ContentCell>
        </SortableRow>
    );
};

export default SortableValueRow;
