To get all metadata from Snowflake you need to use two plugins `snowflake` and `snowflake-usage`. Both of them are described in this page. These will require 2 separate recipes.


We encourage you to try out new `snowflake-beta` plugin as alternative to running both `snowflake` and `snowflake-usage` plugins and share feedback. `snowflake-beta` is much faster than `snowflake` for extracting metadata . Please note that, `snowflake-beta` plugin currently does not support column level profiling, unlike `snowflake` plugin.