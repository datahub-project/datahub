import React, { useState } from 'react';
import { InfoCircleOutlined } from '@ant-design/icons';
import { Divider, message, Modal, Popover, Tooltip, Typography } from 'antd';
import { blue } from '@ant-design/colors';
import styled from 'styled-components';
import moment from 'moment';
import { Deprecation } from '../../../../../types.generated';
import { getLocaleTimezone } from '../../../../shared/time/timeUtils';
import { ANTD_GRAY } from '../../constants';
import { useBatchUpdateDeprecationMutation } from '../../../../../graphql/mutations.generated';
import { Editor } from '../../tabs/Documentation/components/editor/Editor';
import StripMarkdownText, { removeMarkdown } from './StripMarkdownText';

const DeprecatedContainer = styled.div`
    height: 18px;
    border: 1px solid #cd0d24;
    border-radius: 15px;
    display: flex;
    justify-content: center;
    align-items: center;
    color: #cd0d24;
    padding-top: 8px;
    padding-bottom: 8px;
    padding-right: 4px;
    padding-left: 4px;
`;

const DeprecatedText = styled.div`
    padding-right: 2px;
    padding-left: 2px;
    font-size: 10px;
`;

const DeprecatedTitle = styled(Typography.Text)`
    display: block;
    font-size: 14px;
    margin-bottom: 5px;
    font-weight: bold;
`;

const LastEvaluatedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[7]};
`;

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

const UndeprecatedIcon = styled(InfoCircleOutlined)`
    font-size: 14px;
    padding-right: 6px;
`;

const IconGroup = styled.div`
    font-size: 12px;
    color: 'black';
    &:hover {
        color: ${blue[4]};
        cursor: pointer;
    }
`;

const DescriptionContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    width: 100%;
    height: 100%;
    min-height: 22px;
    margin-bottom: 14px;
`;
const StyledViewer = styled(Editor)`
    padding-right: 8px;
    display: block;

    .remirror-editor.ProseMirror {
        padding: 0;
    }
`;

const ExpandedActions = styled.div`
    height: 10px;
`;
const ReadLessText = styled(Typography.Link)`
    margin-right: 4px;
`;
type Props = {
    urn: string;
    deprecation: Deprecation;
    refetch?: () => void;
    showUndeprecate: boolean | null;
};
const ABBREVIATED_LIMIT = 80;

export const DeprecationPill = ({ deprecation, urn, refetch, showUndeprecate }: Props) => {
    const [batchUpdateDeprecationMutation] = useBatchUpdateDeprecationMutation();
    const [expanded, setExpanded] = useState(false);
    const overLimit = deprecation?.note && removeMarkdown(deprecation?.note).length > 80;
    /**
     * Deprecation Decommission Timestamp
     */
    const localeTimezone = getLocaleTimezone();

    let decommissionTimeSeconds;
    if (deprecation.decommissionTime) {
        if (deprecation.decommissionTime < 943920000000) {
            // Time is set in way past if it was milli-second so considering this as set in seconds
            decommissionTimeSeconds = deprecation.decommissionTime;
        } else {
            decommissionTimeSeconds = deprecation.decommissionTime / 1000;
        }
    }
    const decommissionTimeLocal =
        (decommissionTimeSeconds &&
            `Scheduled to be decommissioned on ${moment
                .unix(decommissionTimeSeconds)
                .format('DD/MMM/YYYY')} (${localeTimezone})`) ||
        undefined;
    const decommissionTimeGMT =
        decommissionTimeSeconds && moment.unix(decommissionTimeSeconds).utc().format('dddd, DD/MMM/YYYY HH:mm:ss z');

    const hasDetails = deprecation.note !== '' || deprecation.decommissionTime !== null;
    const isDividerNeeded = deprecation.note !== '' && deprecation.decommissionTime !== null;

    const batchUndeprecate = () => {
        batchUpdateDeprecationMutation({
            variables: {
                input: {
                    resources: [{ resourceUrn: urn }],
                    deprecated: false,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Marked assets as un-deprecated!', duration: 2 });
                    refetch?.();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to mark assets as un-deprecated: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    return (
        <Popover
            overlayStyle={{ maxWidth: 480 }}
            placement="right"
            content={
                hasDetails ? (
                    <>
                        {deprecation?.note !== '' && <DeprecatedTitle>Deprecation note</DeprecatedTitle>}
                        {isDividerNeeded && <ThinDivider />}
                        <DescriptionContainer>
                            {expanded || !overLimit ? (
                                <>
                                    {deprecation?.note && deprecation?.note !== '' && (
                                        <>
                                            <StyledViewer content={deprecation.note} readOnly />
                                            <ExpandedActions>
                                                {overLimit && (
                                                    <ReadLessText
                                                        onClick={() => {
                                                            setExpanded(false);
                                                        }}
                                                    >
                                                        Read Less
                                                    </ReadLessText>
                                                )}
                                            </ExpandedActions>
                                        </>
                                    )}
                                </>
                            ) : (
                                <>
                                    <StripMarkdownText
                                        limit={ABBREVIATED_LIMIT}
                                        readMore={
                                            <>
                                                <Typography.Link
                                                    onClick={() => {
                                                        setExpanded(true);
                                                    }}
                                                >
                                                    Read More
                                                </Typography.Link>
                                            </>
                                        }
                                        shouldWrap
                                    >
                                        {deprecation.note}
                                    </StripMarkdownText>
                                </>
                            )}
                        </DescriptionContainer>
                        {deprecation?.decommissionTime !== null && (
                            <Typography.Text type="secondary">
                                <Tooltip placement="right" title={decommissionTimeGMT}>
                                    <LastEvaluatedAtLabel>{decommissionTimeLocal}</LastEvaluatedAtLabel>
                                </Tooltip>
                            </Typography.Text>
                        )}
                        {isDividerNeeded && <ThinDivider />}
                        {showUndeprecate && (
                            <IconGroup
                                onClick={() =>
                                    Modal.confirm({
                                        title: `Confirm Mark as un-deprecated`,
                                        content: `Are you sure you want to mark this asset as un-deprecated?`,
                                        onOk() {
                                            batchUndeprecate();
                                        },
                                        onCancel() {},
                                        okText: 'Yes',
                                        maskClosable: true,
                                        closable: true,
                                    })
                                }
                            >
                                <UndeprecatedIcon />
                                Mark as un-deprecated
                            </IconGroup>
                        )}
                    </>
                ) : (
                    'No additional details'
                )
            }
        >
            <DeprecatedContainer>
                <DeprecatedText>DEPRECATED</DeprecatedText>
            </DeprecatedContainer>
        </Popover>
    );
};
