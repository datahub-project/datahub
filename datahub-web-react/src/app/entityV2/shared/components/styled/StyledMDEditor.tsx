/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import MDEditor from '@uiw/react-md-editor';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

export default styled(MDEditor)`
    height: calc(100% - 46px) !important;
    z-index: 0;
    box-shadow: none;
    border-radius: 0;
    font-weight: 400;
    .w-md-editor-toolbar {
        border-color: ${ANTD_GRAY[4]};
        background: white;
        padding: 0 20px;
        height: 46px !important;
        li {
            button {
                height: 100%;
                margin: 0 5px;
            }
            svg {
                width: 16px;
                height: 16px;
            }
            &.active > button {
                color: ${(props) => props.theme.styles['primary-color']};
                background-color: ${ANTD_GRAY[3]};
            }
        }
    }
    .w-md-editor-preview {
        box-shadow: inset 1px 0 0 0 ${ANTD_GRAY[4]};
    }
    .w-md-editor-content {
        height: calc(100% - 46px) !important;
    }
`;
