# Classification

The classification feature enables sources to be configured to automatically predict info types for columns and use them as glossary terms. This is an explicit opt-in feature and is not enabled by default.

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field | Required | Type | Description | Default |
| ---   | ---      | ---  | --- | -- |
| enabled |  | boolean | Whether classification should be used to auto-detect glossary terms | False |
| info_type_to_term |  | Dict[str,string] | Optional mapping to provide glossary term identifier for info type.  | By default, info type is used as glossary term identifier. |
| classifiers |  | Array of object | Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance. | [{'type': 'datahub', 'config': None}] |
| table_pattern |  | AllowDenyPattern (see below for fields) | Regex patterns to filter tables for classification. This is used in combination with other patterns in parent config. Specify regex to match the entire table name in `database.schema.table` format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*' | {'allow': ['.*'], 'deny': [], 'ignoreCase': True} |
| table_pattern.allow |  | Array of string | List of regex patterns to include in ingestion | ['.*'] |
| table_pattern.deny |  | Array of string | List of regex patterns to exclude from ingestion. | [] |
| table_pattern.ignoreCase |  | boolean | Whether to ignore case sensitivity during pattern matching. | True |
| column_pattern |  | AllowDenyPattern (see below for fields) | Regex patterns to filter columns for classification. This is used in combination with other patterns in parent config. Specify regex to match the column name in `database.schema.table.column` format. | {'allow': ['.*'], 'deny': [], 'ignoreCase': True} |
| column_pattern.allow |  | Array of string | List of regex patterns to include in ingestion | ['.*'] |
| column_pattern.deny |  | Array of string | List of regex patterns to exclude from ingestion. | [] |
| column_pattern.ignoreCase |  | boolean | Whether to ignore case sensitivity during pattern matching. | True |

## DataHub Classifier

DataHub Classifier is the default classifier implementation, which uses [acryl-datahub-classify](https://pypi.org/project/acryl-datahub-classify/) library to predict info types.

### Config Details

| Field | Required | Type | Description | Default |
| ---   | ---      | ---  | --- | -- |
| confidence_level_threshold |  | number |  | 0.6 |
| info_types |  | list[string] | List of infotypes to be predicted. By default, all supported infotypes are considered. If specified. this should be subset of ['Email_Address', 'Gender', 'Credit_Debit_Card_Number', 'Phone_Number', 'Street_Address', 'Full_Name', 'Age', 'IBAN', 'US_Social_Security_Number', 'Vehicle_Identification_Number', 'IP_Address_v4', 'IP_Address_v6', 'US_Driving_License_Number', 'Swift_Code'] | None |
| info_types_config | Configuration details for infotypes | Dict[str, InfoTypeConfig] |  | See [reference_input.py](https://github.com/acryldata/datahub-classify/blob/main/datahub-classify/src/datahub_classify/reference_input.py) for default configuration. |
| info_types_config.`key`.prediction_factors_and_weights | ❓ (required if info_types_config.`key` is set) | Dict[str,number] | Factors and their weights to consider when predicting info types |  |
| info_types_config.`key`.name |  | NameFactorConfig (see below for fields) |  |  |
| info_types_config.`key`.name.regex |  | Array of string | List of regex patterns the column name follows for the info type | ['.*'] |
| info_types_config.`key`.description |  | DescriptionFactorConfig (see below for fields) |  |  |
| info_types_config.`key`.description.regex |  | Array of string | List of regex patterns the column description follows for the info type | ['.*'] |
| info_types_config.`key`.datatype |  | DataTypeFactorConfig (see below for fields) |  |  |
| info_types_config.`key`.datatype.type |  | Array of string | List of data types for the info type | ['.*'] |
| info_types_config.`key`.values |  | ValuesFactorConfig (see below for fields) |  |  |
| info_types_config.`key`.values.prediction_type | ❓ (required if info_types_config.`key`.values is set) | string |  | None |
| info_types_config.`key`.values.regex |  | Array of string | List of regex patterns the column value follows for the info type | None |
| info_types_config.`key`.values.library |  | Array of string | Library used for prediction | None |

### Supported sources

* snowflake

#### Example

```yml
source:
  type: snowflake
  config:
    env: PROD
    # Coordinates
    account_id: account_name
    warehouse: "COMPUTE_WH"

    # Credentials
    username: user
    password: pass
    role: "sysadmin"

    # Options
    top_n_queries: 10
    email_domain: mycompany.com

    classification:
      enabled: True
      classifiers:
        - type: datahub          
```

#### Example with Advanced Configuration: Specifying custom info_types_config

```yml
source:
  type: snowflake
  config:
    env: PROD
    # Coordinates
    account_id: account_name
    warehouse: "COMPUTE_WH"

    # Credentials
    username: user
    password: pass
    role: "sysadmin"

    # Options
    top_n_queries: 10
    email_domain: mycompany.com

    classification:
      enabled: True
      info_type_to_term:
        Email_Address: "Email"
      classifiers:
        - type: datahub          
          config:
            confidence_level_threshold: 0.7
            info_types_config:
              Email_Address:
                prediction_factors_and_weights:
                  name: 0.4
                  description: 0
                  datatype: 0
                  values: 0.6
                name:
                  regex:
                    - "^.*mail.*id.*$"
                    - "^.*id.*mail.*$"
                    - "^.*mail.*add.*$"
                    - "^.*add.*mail.*$"
                    - email
                    - mail
                description:
                  regex:
                    - "^.*mail.*id.*$"
                    - "^.*mail.*add.*$"
                    - email
                    - mail
                datatype:
                  type:
                    - str
                values:
                  prediction_type: regex
                  regex:
                    - "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}"
                  library: []
              Gender:
                prediction_factors_and_weights:
                  name: 0.4
                  description: 0
                  datatype: 0
                  values: 0.6
                name:
                  regex:
                    - "^.*gender.*$"
                    - "^.*sex.*$"
                    - gender
                    - sex
                description:
                  regex:
                    - "^.*gender.*$"
                    - "^.*sex.*$"
                    - gender
                    - sex
                datatype:
                  type:
                    - int
                    - str
                values:
                  prediction_type: regex
                  regex:
                    - male
                    - female
                    - man
                    - woman
                    - m
                    - f
                    - w
                    - men
                    - women
                  library: []
              Credit_Debit_Card_Number:
                prediction_factors_and_weights:
                  name: 0.4
                  description: 0
                  datatype: 0
                  values: 0.6
                name:
                  regex:
                    - "^.*card.*number.*$"
                    - "^.*number.*card.*$"
                    - "^.*credit.*card.*$"
                    - "^.*debit.*card.*$"
                description:
                  regex:
                    - "^.*card.*number.*$"
                    - "^.*number.*card.*$"
                    - "^.*credit.*card.*$"
                    - "^.*debit.*card.*$"
                datatype:
                  type:
                    - str
                    - int
                values:
                  prediction_type: regex
                  regex:
                    - "^4[0-9]{12}(?:[0-9]{3})?$"
                    - "^(?:5[1-5][0-9]{2}|222[1-9]|22[3-9][0-9]|2[3-6][0-9]{2}|27[01][0-9]|2720)[0-9]{12}$"
                    - "^3[47][0-9]{13}$"
                    - "^3(?:0[0-5]|[68][0-9])[0-9]{11}$"
                    - "^6(?:011|5[0-9]{2})[0-9]{12}$"
                    - "^(?:2131|1800|35\\d{3})\\d{11}$"
                    - "^(6541|6556)[0-9]{12}$"
                    - "^389[0-9]{11}$"
                    - "^63[7-9][0-9]{13}$"
                    - "^9[0-9]{15}$"
                    - "^(6304|6706|6709|6771)[0-9]{12,15}$"
                    - "^(5018|5020|5038|6304|6759|6761|6763)[0-9]{8,15}$"
                    - "^(62[0-9]{14,17})$"
                    - "^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14})$"
                    - "^(4903|4905|4911|4936|6333|6759)[0-9]{12}|(4903|4905|4911|4936|6333|6759)[0-9]{14}|(4903|4905|4911|4936|6333|6759)[0-9]{15}|564182[0-9]{10}|564182[0-9]{12}|564182[0-9]{13}|633110[0-9]{10}|633110[0-9]{12}|633110[0-9]{13}$"
                    - "^(6334|6767)[0-9]{12}|(6334|6767)[0-9]{14}|(6334|6767)[0-9]{15}$"
                  library: []
              Phone_Number:
                prediction_factors_and_weights:
                  name: 0.4
                  description: 0
                  datatype: 0
                  values: 0.6
                name:
                  regex:
                    - ".*phone.*(num|no).*"
                    - ".*(num|no).*phone.*"
                    - ".*[^a-z]+ph[^a-z]+.*(num|no).*"
                    - ".*(num|no).*[^a-z]+ph[^a-z]+.*"
                    - ".*mobile.*(num|no).*"
                    - ".*(num|no).*mobile.*"
                    - ".*telephone.*(num|no).*"
                    - ".*(num|no).*telephone.*"
                    - ".*cell.*(num|no).*"
                    - ".*(num|no).*cell.*"
                    - ".*contact.*(num|no).*"
                    - ".*(num|no).*contact.*"
                    - ".*landline.*(num|no).*"
                    - ".*(num|no).*landline.*"
                    - ".*fax.*(num|no).*"
                    - ".*(num|no).*fax.*"
                    - phone
                    - telephone
                    - landline
                    - mobile
                    - tel
                    - fax
                    - cell
                    - contact
                description:
                  regex:
                    - ".*phone.*(num|no).*"
                    - ".*(num|no).*phone.*"
                    - ".*[^a-z]+ph[^a-z]+.*(num|no).*"
                    - ".*(num|no).*[^a-z]+ph[^a-z]+.*"
                    - ".*mobile.*(num|no).*"
                    - ".*(num|no).*mobile.*"
                    - ".*telephone.*(num|no).*"
                    - ".*(num|no).*telephone.*"
                    - ".*cell.*(num|no).*"
                    - ".*(num|no).*cell.*"
                    - ".*contact.*(num|no).*"
                    - ".*(num|no).*contact.*"
                    - ".*landline.*(num|no).*"
                    - ".*(num|no).*landline.*"
                    - ".*fax.*(num|no).*"
                    - ".*(num|no).*fax.*"
                    - phone
                    - telephone
                    - landline
                    - mobile
                    - tel
                    - fax
                    - cell
                    - contact
                datatype:
                  type:
                    - int
                    - str
                values:
                  prediction_type: library
                  regex: []
                  library:
                    - phonenumbers
              Street_Address:
                prediction_factors_and_weights:
                  name: 0.5
                  description: 0
                  datatype: 0
                  values: 0.5
                name:
                  regex:
                    - ".*street.*add.*"
                    - ".*add.*street.*"
                    - ".*full.*add.*"
                    - ".*add.*full.*"
                    - ".*mail.*add.*"
                    - ".*add.*mail.*"
                    - add[^a-z]+
                    - address
                    - street
                description:
                  regex:
                    - ".*street.*add.*"
                    - ".*add.*street.*"
                    - ".*full.*add.*"
                    - ".*add.*full.*"
                    - ".*mail.*add.*"
                    - ".*add.*mail.*"
                    - add[^a-z]+
                    - address
                    - street
                datatype:
                  type:
                    - str
                values:
                  prediction_type: library
                  regex: []
                  library:
                    - spacy
              Full_name:
                prediction_factors_and_weights:
                  name: 0.3
                  description: 0
                  datatype: 0
                  values: 0.7
                name:
                  regex:
                    - ".*person.*name.*"
                    - ".*name.*person.*"
                    - ".*user.*name.*"
                    - ".*name.*user.*"
                    - ".*full.*name.*"
                    - ".*name.*full.*"
                    - fullname
                    - name
                    - person
                    - user
                description:
                  regex:
                    - ".*person.*name.*"
                    - ".*name.*person.*"
                    - ".*user.*name.*"
                    - ".*name.*user.*"
                    - ".*full.*name.*"
                    - ".*name.*full.*"
                    - fullname
                    - name
                    - person
                    - user
                datatype:
                  type:
                    - str
                values:
                  prediction_type: library
                  regex: []
                  library:
                    - spacy
              Age:
                prediction_factors_and_weights:
                  name: 0.65
                  description: 0
                  datatype: 0
                  values: 0.35
                name:
                  regex:
                    - age[^a-z]+.*
                    - ".*[^a-z]+age"
                    - ".*[^a-z]+age[^a-z]+.*"
                    - age
                description:
                  regex:
                    - age[^a-z]+.*
                    - ".*[^a-z]+age"
                    - ".*[^a-z]+age[^a-z]+.*"
                    - age
                datatype:
                  type:
                    - int
                values:
                  prediction_type: library
                  regex: []
                  library:
                    - rule_based_logic

```
