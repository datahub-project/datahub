import styled, { css } from 'styled-components';

import { DividerType } from '@components/components/Divider/types';

// Margins and sizing match antd's Divider so replacing it keeps existing layouts unchanged.
export const StyledDivider = styled.div<{ $type: DividerType; $dashed: boolean }>`
    box-sizing: border-box;
    padding: 0;
    border: 0 ${(props) => (props.$dashed ? 'dashed' : 'solid')} ${(props) => props.theme.colors.border};

    ${(props) =>
        props.$type === 'vertical'
            ? css`
                  position: relative;
                  top: -0.06em;
                  display: inline-block;
                  height: 0.9em;
                  margin: 0 8px;
                  vertical-align: middle;
                  border-left-width: 1px;
              `
            : css`
                  display: flex;
                  clear: both;
                  width: 100%;
                  min-width: 100%;
                  margin: 24px 0;
                  border-top-width: 1px;
              `}
`;
