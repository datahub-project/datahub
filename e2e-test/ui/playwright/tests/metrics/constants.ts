/**
 * URNs and names seeded by fixtures/data.json for the Metrics Playwright suite.
 */

import { THEME_V2_FLAGS } from '../../utils/test-feature-flags';

export const METRICS_FEATURE_FLAGS = {
  ...THEME_V2_FLAGS,
  metricsEnabled: true,
} as const;

export const METRICS_FEATURE_FLAGS_OFF = {
  ...THEME_V2_FLAGS,
  metricsEnabled: false,
} as const;

const PREFIX = 'pw_metrics';

export const DOMAIN_SALES_URN = `urn:li:domain:${PREFIX}-sales`;
export const DOMAIN_FINANCE_URN = `urn:li:domain:${PREFIX}-finance`;

export const SEMANTIC_MODEL_ORDERS_URN = `urn:li:semanticModel:(urn:li:dataPlatform:snowflake,${PREFIX}.analytics,orders_model)`;
export const SEMANTIC_MODEL_PAYMENTS_URN = `urn:li:semanticModel:(urn:li:dataPlatform:bigquery,${PREFIX}.finance,payments_model)`;

export const ORDERS_LOGICAL_URN = `urn:li:dataset:(urn:li:dataPlatform:snowflake,${PREFIX}.analytics.orders_model.orders_ds,PROD)`;
export const CUSTOMERS_LOGICAL_URN = `urn:li:dataset:(urn:li:dataPlatform:snowflake,${PREFIX}.analytics.orders_model.customers_ds,PROD)`;
export const PAYMENTS_LOGICAL_URN = `urn:li:dataset:(urn:li:dataPlatform:bigquery,${PREFIX}.finance.payments_model.payments_ds,PROD)`;

export const PHYS_ORDERS_URN = `urn:li:dataset:(urn:li:dataPlatform:snowflake,${PREFIX}.raw.orders,PROD)`;
export const PHYS_CUSTOMERS_URN = `urn:li:dataset:(urn:li:dataPlatform:snowflake,${PREFIX}.raw.customers,PROD)`;
export const PHYS_PAYMENTS_URN = `urn:li:dataset:(urn:li:dataPlatform:bigquery,${PREFIX}.raw.payments,PROD)`;
export const PHYS_STANDALONE_URN = `urn:li:dataset:(urn:li:dataPlatform:snowflake,${PREFIX}.raw.standalone_events,PROD)`;

export const TOTAL_REVENUE_URN = `urn:li:metric:(urn:li:dataPlatform:snowflake,${PREFIX}.analytics,total_revenue)`;
export const DOUBLE_REVENUE_URN = `urn:li:metric:(urn:li:dataPlatform:snowflake,${PREFIX}.analytics,double_revenue)`;
export const CHILD_REVENUE_URN = `urn:li:metric:(urn:li:dataPlatform:snowflake,${PREFIX}.analytics,child_revenue)`;
export const ORDER_COUNT_URN = `urn:li:metric:(urn:li:dataPlatform:snowflake,${PREFIX}.analytics,order_count)`;
export const REVENUE_PER_CUSTOMER_URN = `urn:li:metric:(urn:li:dataPlatform:snowflake,${PREFIX}.analytics,revenue_per_customer)`;
export const PAYMENT_VOLUME_URN = `urn:li:metric:(urn:li:dataPlatform:bigquery,${PREFIX}.finance,payment_volume)`;
export const STANDALONE_EVENT_COUNT_URN = `urn:li:metric:(urn:li:dataPlatform:snowflake,${PREFIX}.standalone,event_count)`;

export const ORDERS_CHART_URN = `urn:li:chart:(looker,${PREFIX}.orders_chart)`;
export const ORDERS_DASHBOARD_URN = `urn:li:dashboard:(looker,${PREFIX}.orders_dashboard)`;

export const NAMES = {
  ORDERS_MODEL: 'PW Orders Model',
  PAYMENTS_MODEL: 'PW Payments Model',
  TOTAL_REVENUE: 'PW Total Revenue',
  DOUBLE_REVENUE: 'PW Double Revenue',
  CHILD_REVENUE: 'PW Child Revenue',
  ORDER_COUNT: 'PW Order Count',
  REVENUE_PER_CUSTOMER: 'PW Revenue Per Customer',
  PAYMENT_VOLUME: 'PW Payment Volume',
  EVENT_COUNT: 'PW Event Count',
  ORDERS_ALIAS: 'ORDERS',
  CUSTOMERS_ALIAS: 'CUSTOMERS',
} as const;

/** Expected seeded inventory used by home page count cards. */
export const EXPECTED_COUNTS = {
  SEMANTIC_MODELS: 2,
  /** Root metrics = metrics with no parentMetric (excludes child_revenue). */
  ROOT_METRICS: 6,
  PLATFORMS: 2,
} as const;
