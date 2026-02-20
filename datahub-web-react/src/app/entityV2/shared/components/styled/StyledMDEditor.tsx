import MDEditor from '@uiw/react-md-editor';
import styled from 'styled-components';

export default styled(MDEditor)`
    height: calc(100% - 46px) !important;
    z-index: 0;
    box-shadow: none;
    border-radius: 0;
    font-weight: 400;
    .w-md-editor-toolbar {
        border-color: ${(props) => props.theme.colors.bgSurface};
        background: ${(props) => props.theme.colors.bgSurface};
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
                color: ${(props) => props.theme.colors.textActive};
                background-color: ${(props) => props.theme.colors.bgSurface};
            }
        }
    }
    .w-md-editor-preview {
        box-shadow: inset 1px 0 0 0 ${(props) => props.theme.colors.bgSurface};
    }
    .w-md-editor-content {
        height: calc(100% - 46px) !important;
    }
`;
