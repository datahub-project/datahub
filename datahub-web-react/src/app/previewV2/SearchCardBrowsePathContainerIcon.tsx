import { FolderOpenOutlined } from '@ant-design/icons';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';
import { Container } from '../../types.generated';
import { getSubTypeIcon } from '../entityV2/shared/components/subtypes';
import { SEARCH_COLORS } from '../entityV2/shared/constants';

const IconWrapper = styled.span`
    img,
    svg {
        height: 12px;
    }
    color: ${SEARCH_COLORS.PLATFORM_TEXT};
    margin-right: 6px;
`;

const DefaultIcon = styled(FolderOpenOutlined)`
    color: ${SEARCH_COLORS.PLATFORM_TEXT};

    &&& {
        font-size: 14px;
    }
`;

interface Props {
    container: Maybe<Container>;
}

function ContainerIcon({ container }: Props) {
    const subType = container?.subTypes?.typeNames?.[0].toLowerCase();

    return <IconWrapper>{(subType && getSubTypeIcon(subType)) || <DefaultIcon />}</IconWrapper>;
}

export default ContainerIcon;
