/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

// Helper functions to simplify logic
const getBackgroundColor = ({ isExpandedView, primary }: { isExpandedView?: boolean; primary?: boolean }) => {
    if (isExpandedView) return 'inherit';
    return primary ? '#5280e8' : '#ffffff';
};

const getColor = ({ isExpandedView, primary }: { isExpandedView?: boolean; primary?: boolean }) => {
    if (primary) return '#fff';
    return isExpandedView ? '#000' : '#5280e8';
};

const getBorder = ({ isExpandedView, primary }: { isExpandedView?: boolean; primary?: boolean }) => {
    if (isExpandedView) return 'none';
    return primary ? '1px solid #5280e8' : '1px solid #f0f0f0';
};

export const ActionItemButton = styled.button<{ disabled?: boolean; primary?: boolean; isExpandedView?: boolean }>`
    border-radius: ${(props) => (props.isExpandedView ? `0px` : `20px`)};
    width: 28px;
    height: 28px;
    margin: ${(props) => (props.isExpandedView ? `0px` : `0px 4px`)};
    padding: 0px;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    background-color: ${(props) => getBackgroundColor(props)};
    color: ${(props) => getColor(props)};
    box-shadow: none;
    cursor: ${(props) => (props.disabled ? 'not-allowed' : 'pointer')};
    &&:hover {
        ${(props) => !props.disabled && 'opacity: 0.8;'}
    }
    ${(props) => props.disabled && 'opacity: 0.5'};
    border: ${(props) => getBorder(props)};
`;
