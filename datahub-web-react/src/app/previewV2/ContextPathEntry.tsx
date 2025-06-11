import { debounce } from 'lodash';
import React, { useCallback } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import ContextPathEntityIcon from '@app/previewV2/ContextPathEntityIcon';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import { colors } from '@src/alchemy-components';

const Path = styled.div<{ isLast: boolean }>`
    flex: ${({ isLast }) => (isLast ? '1 0 1' : '1 1 0')};
    max-width: max-content;
    min-width: 16px;
    overflow: hidden;

    font-style: normal;
    font-weight: 500;
    display: flex;
    align-items: center;
`;

const ContainerText = styled.span`
    display: inline-block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const Contents = styled.div<{ $disabled?: boolean }>`
    border-radius: 4px;
    overflow: hidden;
    display: flex;
    gap: 4px;
    align-items: center;
    line-height: 22px;
    color: ${colors.gray[1700]};

    && svg {
        color: ${colors.gray[1700]};
    }

    :hover {
        color: ${({ $disabled }) => ($disabled ? colors.gray[1700] : colors.violet[500])};
        cursor: ${({ $disabled }) => ($disabled ? 'default' : 'pointer')};

        && svg {
            color: ${({ $disabled }) => ($disabled ? colors.gray[1700] : colors.violet[500])};
        }
    }
`;

interface Props {
    name?: string;
    linkUrl?: string;
    isLast: boolean;
    hideIcon?: boolean;
    linkDisabled?: boolean;
    setIsTruncated: (v: boolean) => void;
    className?: string;
}

function ContextPathEntry(props: Props) {
    const { name, linkUrl, isLast, hideIcon, linkDisabled, setIsTruncated, className } = props;
    const linkProps = useEmbeddedProfileLinkProps();

    const handleResize: ResizeObserverCallback = useCallback(
        (entries) => {
            if (!entries || entries.length === 0) return;
            const node = entries[0].target;
            setIsTruncated(node.scrollWidth > node.clientWidth);
        },
        [setIsTruncated],
    );

    const measuredRef = useCallback(
        (node: HTMLDivElement | null) => {
            if (node !== null) {
                const resizeObserver = new ResizeObserver(debounce(handleResize, 100));
                resizeObserver.observe(node);
            }
        },
        [handleResize],
    );

    const showLink = !!linkUrl && !linkDisabled;
    const contents = (
        <Contents $disabled={!showLink}>
            {!hideIcon && linkUrl && <ContextPathEntityIcon />}
            <ContainerText ref={measuredRef}>{name}</ContainerText>
        </Contents>
    );

    return (
        <Path isLast={isLast} className={className}>
            {showLink ? (
                <Link to={linkUrl} data-testid="container" {...linkProps}>
                    {contents}
                </Link>
            ) : (
                contents
            )}
        </Path>
    );
}

export default ContextPathEntry;
