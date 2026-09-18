import { Button } from '@components';
import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { DotsSixVertical } from '@phosphor-icons/react/dist/csr/DotsSixVertical';
import React from 'react';
import styled from 'styled-components';

// Value, actions, handle. The value column starts at the container's left edge so the input lines
// up with the other fields on the page. The description sits on a second row under the value input,
// while actions/handle span both rows so `align-items: center` centers them against the full row
// height instead of pinning them to the value input.
const SortableRow = styled.div<{
    $isDragging: boolean;
    $hasDescription: boolean;
    $isDisabled: boolean;
    $transform?: string;
    $transition?: string;
}>`
    display: grid;
    grid-template-columns: 1fr auto ${({ $isDisabled }) => ($isDisabled ? '0' : '20px')};
    ${({ $hasDescription }) =>
        $hasDescription
            ? `grid-template-areas:
                   'value actions handle'
                   'description actions handle';`
            : `grid-template-areas: 'value actions handle';`}
    align-items: center;
    column-gap: 8px;
    row-gap: 8px;
    /* Vertical padding sits between the content and the divider, so a value is closer to its own
       description (row-gap) than to the neighbouring pair. The first row omits it so the list keeps
       the standard 4px label gap. */
    padding: 16px 0;

    &:first-child {
        padding-top: 0;
    }
    border-radius: 6px;
    background-color: ${(props) => (props.$isDragging ? props.theme.colors.bgSurface : 'transparent')};
    box-shadow: ${(props) => (props.$isDragging ? props.theme.colors.shadowSm : 'none')};
    z-index: ${(props) => (props.$isDragging ? '999' : 'auto')};
    position: relative;

    /* Separates one value from the next, so it is drawn as its own line rather than as a border on
       the row, whose corner radius would bow its ends. */
    &:not(:last-child)::after {
        content: '';
        position: absolute;
        left: 0;
        right: 0;
        bottom: 0;
        height: 1px;
        background-color: ${(props) => props.theme.colors.border};
        opacity: ${(props) => (props.$isDragging ? '0' : '1')};
    }
    transform: ${(props) => props.$transform};
    transition: ${(props) => props.$transition};
`;

const HandleCell = styled.div`
    grid-area: handle;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const DragButton = styled(Button)<{ $isDragging: boolean }>`
    cursor: ${(props) => (props.$isDragging ? 'grabbing' : 'grab')};
    color: ${(props) => props.theme.colors.textTertiary};

    &:hover {
        color: ${(props) => props.theme.colors.text};
    }
`;

const ValueCell = styled.div`
    grid-area: value;
    min-width: 0;
`;

const ActionsCell = styled.div`
    grid-area: actions;
    display: flex;
    align-items: center;
    gap: 4px;
`;

const DescriptionCell = styled.div`
    grid-area: description;
    min-width: 0;
`;

type Props = {
    id: string;
    isDisabled?: boolean;
    dragLabel: string;
    valueInput: React.ReactNode;
    descriptionInput?: React.ReactNode;
    actions?: React.ReactNode;
};

const SortableAllowedValue = ({ id, isDisabled, dragLabel, valueInput, descriptionInput, actions }: Props) => {
    const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
        id,
        disabled: isDisabled,
    });

    return (
        <SortableRow
            ref={setNodeRef}
            {...attributes}
            $isDragging={isDragging}
            $hasDescription={!!descriptionInput}
            $isDisabled={!!isDisabled}
            $transform={CSS.Transform.toString(transform)}
            $transition={transition}
        >
            <ValueCell>{valueInput}</ValueCell>
            {descriptionInput && <DescriptionCell>{descriptionInput}</DescriptionCell>}
            <ActionsCell>{actions}</ActionsCell>
            <HandleCell>
                {!isDisabled && (
                    <DragButton
                        {...listeners}
                        aria-label={dragLabel}
                        variant="text"
                        isCircle
                        icon={{ icon: DotsSixVertical, size: 'lg' }}
                        $isDragging={isDragging}
                    />
                )}
            </HandleCell>
        </SortableRow>
    );
};

export default SortableAllowedValue;
