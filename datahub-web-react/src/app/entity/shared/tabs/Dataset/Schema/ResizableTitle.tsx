import React, { useState } from 'react';
import { Resizable } from 'react-resizable';
import PropTypes from 'prop-types';
import styled from 'styled-components';

const ResizableDiv = styled.div`
    width: 2px;
    height: 100%;
    position: absolute;
    right: 0;
    top: 0;
    cursor: col-resize;
    z-index: 1;
`;

const ColumnHeader = styled.th`
    font-weight: 600 !important;
    color: #595959 !important;
    word-break: normal !important;
`;

const MIN_COLUMN_WIDTH = 50;
const MIN_COLUMN_HEIGHT = 30;

const parseWidth = (width: any) => {
    if (width === undefined) return MIN_COLUMN_WIDTH;
    if (typeof width === 'string' && width.endsWith('%')) {
        const percentage = parseFloat(width.slice(0, -1));
        if (!Number.isNaN(percentage)) {
            return (percentage / 100) * window.innerWidth; // Convert percentage to pixel value
        }
    }
    return width;
};

export const ResizableTitle = ({ onResize, width, column, onClick, ...restProps }) => {
    const [allowClick, setAllowClick] = useState(true);

    if (!width) {
        return <th {...restProps}>{restProps.title}</th>;
    }

    const numericWidth = parseWidth(width);
    const title = column && column.title;

    return (
        <Resizable
            width={numericWidth}
            height={0}
            onResize={onResize}
            onMouseDown={() => {
                setAllowClick(true);
            }}
            onResizeStart={() => {
                setAllowClick(false);
            }}
            onResizeStop={() => {
                setAllowClick(true);
            }}
            onClick={(e) => allowClick && onClick !== undefined && onClick(e)}
            minConstraints={[MIN_COLUMN_WIDTH, MIN_COLUMN_HEIGHT]}
            handle={
                <ResizableDiv
                    onClick={(e) => {
                        e.stopPropagation();
                    }}
                />
            }
            draggableOpts={{ enableUserSelectHack: false }}
        >
            <ColumnHeader className={column?.sorter !== undefined ? 'ant-table-column-has-sorters' : ''}>
                {title}
            </ColumnHeader>
        </Resizable>
    );
};

ResizableTitle.propTypes = {
    onResize: PropTypes.func,
    width: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    column: PropTypes.shape({
        title: PropTypes.node,
        sorter: PropTypes.func,
    }),
    onClick: PropTypes.func,
};
