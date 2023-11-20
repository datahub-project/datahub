import React, { useState } from 'react';
import { Resizable } from 'react-resizable';
import PropTypes from 'prop-types';
import styled from 'styled-components';
import { percentToPixelWidth } from '../../../utils';
import { ANTD_GRAY, SCHEMA_TABLE_MIN_COLUMN_HEIGHT, SCHEMA_TABLE_MIN_COLUMN_WIDTH } from '../../../constants';

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
    color: ${ANTD_GRAY[8]} !important;
    word-break: normal !important;
`;

export const ResizableTitle = ({ onResize, width, column, onClick, ...restProps }) => {
    const [allowClick, setAllowClick] = useState(true);

    const numericWidth = percentToPixelWidth(width);
    const title = column && column.title;

    if (!column || !width) {
        return <th {...restProps} />;
    }

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
            minConstraints={[SCHEMA_TABLE_MIN_COLUMN_WIDTH, SCHEMA_TABLE_MIN_COLUMN_HEIGHT]}
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
