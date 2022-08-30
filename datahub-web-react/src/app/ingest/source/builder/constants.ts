import snowflakeLogo from '../../../../images/snowflakelogo.png';
import bigqueryLogo from '../../../../images/bigquerylogo.png';
import redshiftLogo from '../../../../images/redshiftlogo.png';
import kafkaLogo from '../../../../images/kafkalogo.png';

export const SNOWFLAKE_URN = 'urn:li:dataPlatform:snowflake';
export const SNOWFLAKE = 'snowflake';
export const BIGQUERY_URN = 'urn:li:dataPlatform:bigquery';
export const BIGQUERY = 'bigquery';
export const REDSHIFT_URN = 'urn:li:dataPlatform:redshift';
export const REDSHIFT = 'redshift';
export const KAFKA_URN = 'urn:li:dataPlatform:kafka';
export const KAFKA = 'kafka';

export const SOURCE_URN_TO_LOGO = {
    [SNOWFLAKE_URN]: snowflakeLogo,
    [BIGQUERY_URN]: bigqueryLogo,
    [REDSHIFT_URN]: redshiftLogo,
    [KAFKA_URN]: kafkaLogo,
};

export const SOURCE_TO_SOURCE_URN = {
    [SNOWFLAKE]: SNOWFLAKE_URN,
    [BIGQUERY]: BIGQUERY_URN,
    [REDSHIFT]: REDSHIFT_URN,
    [KAFKA]: KAFKA_URN,
};
