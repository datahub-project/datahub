import { FolderOpenOutlined } from '@ant-design/icons';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';
import { Container } from '../../types.generated';
import { getSubTypeIcon } from '../entityV2/shared/components/subtypes';

const IconWrapper = styled.span`
    img,
    svg {
        height: 12px;
    }
    margin-right: 6px;
`;

const DefaultIcon = styled(FolderOpenOutlined)`
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
