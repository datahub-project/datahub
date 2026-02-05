import { describe, expect, it } from 'vitest';

import formatAskDataHubMessage from '@app/searchV2/utils/formatAskDataHubMessage';

describe('formatAskDataHubMessage', () => {
    describe('queries that should NOT have prefix added', () => {
        it('returns query as-is when it ends with a question mark', () => {
            expect(formatAskDataHubMessage('What tables contain customer data?')).toBe(
                'What tables contain customer data?',
            );
            expect(formatAskDataHubMessage('How do I find revenue metrics?')).toBe('How do I find revenue metrics?');
            expect(formatAskDataHubMessage('customer data?')).toBe('customer data?');
        });

        it('returns query as-is when it starts with question words', () => {
            expect(formatAskDataHubMessage('what is the sales table')).toBe('what is the sales table');
            expect(formatAskDataHubMessage('which datasets have PII')).toBe('which datasets have PII');
            expect(formatAskDataHubMessage('why is this table deprecated')).toBe('why is this table deprecated');
            expect(formatAskDataHubMessage('how do I access this data')).toBe('how do I access this data');
            expect(formatAskDataHubMessage('when was this table created')).toBe('when was this table created');
            expect(formatAskDataHubMessage('where is the customer data stored')).toBe(
                'where is the customer data stored',
            );
            expect(formatAskDataHubMessage('who owns this dataset')).toBe('who owns this dataset');
        });

        it('handles auxiliary verbs as question starters', () => {
            expect(formatAskDataHubMessage('does this table have PII')).toBe('does this table have PII');
            expect(formatAskDataHubMessage('is this dataset production ready')).toBe(
                'is this dataset production ready',
            );
            expect(formatAskDataHubMessage('are there any tables with customer info')).toBe(
                'are there any tables with customer info',
            );
            expect(formatAskDataHubMessage('can I access this data')).toBe('can I access this data');
            expect(formatAskDataHubMessage('could you show me sales data')).toBe('could you show me sales data');
            expect(formatAskDataHubMessage('would this work for analytics')).toBe('would this work for analytics');
            expect(formatAskDataHubMessage('should I use this table')).toBe('should I use this table');
        });

        it('handles imperative/request words', () => {
            expect(formatAskDataHubMessage('tell me about the sales table')).toBe('tell me about the sales table');
            expect(formatAskDataHubMessage('explain the data lineage')).toBe('explain the data lineage');
            expect(formatAskDataHubMessage('describe the customer dataset')).toBe('describe the customer dataset');
            expect(formatAskDataHubMessage('show me tables with revenue')).toBe('show me tables with revenue');
            expect(formatAskDataHubMessage('find datasets with PII')).toBe('find datasets with PII');
            expect(formatAskDataHubMessage('list all customer tables')).toBe('list all customer tables');
            expect(formatAskDataHubMessage('give me information about orders')).toBe(
                'give me information about orders',
            );
        });

        it('is case insensitive for question word detection', () => {
            expect(formatAskDataHubMessage('What is this table')).toBe('What is this table');
            expect(formatAskDataHubMessage('WHAT is this table')).toBe('WHAT is this table');
            expect(formatAskDataHubMessage('How do I find data')).toBe('How do I find data');
        });
    });

    describe('queries that SHOULD have prefix added', () => {
        it('adds prefix for simple keyword searches', () => {
            expect(formatAskDataHubMessage('customer data')).toBe('Help me find data related to customer data');
            expect(formatAskDataHubMessage('sales revenue')).toBe('Help me find data related to sales revenue');
            expect(formatAskDataHubMessage('user_table')).toBe('Help me find data related to user_table');
        });

        it('adds prefix for table/dataset names', () => {
            expect(formatAskDataHubMessage('orders_dim')).toBe('Help me find data related to orders_dim');
            expect(formatAskDataHubMessage('fact_sales')).toBe('Help me find data related to fact_sales');
        });
    });

    describe('edge cases', () => {
        it('returns empty string for empty input', () => {
            expect(formatAskDataHubMessage('')).toBe('');
            expect(formatAskDataHubMessage('   ')).toBe('');
        });

        it('trims whitespace from input', () => {
            expect(formatAskDataHubMessage('  what is this  ')).toBe('what is this');
            expect(formatAskDataHubMessage('  customer data  ')).toBe('Help me find data related to customer data');
        });
    });
});
