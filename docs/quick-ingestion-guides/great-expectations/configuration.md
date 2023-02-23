---
title: Configuration
---
# Configuring Your Great Expectations files

Now that you have installed all the dependecies needed for Great Expectations in [the prior step](setup.md), it's now time to set up the files needed.

## Configure files

**Datasource File**: 
   1. This file provides a way for accessing and interacting with data from a wide variety of source
   systems (PostgreSQL, Hive). The command: `great_expectations datasource new --no-jupyter`, which creates a new datasource file, being its default name datasource_new.
   
<p align="center">
   <img width="75%" alt="Datasource File Creation Command" src="datasource.png"/>
</p>

:::note
For Hive, as it doesn't appear as an option, you would have to select the "other" option.
:::

   2. In order to be able to edit it, you would have to execute the command: `jupyter notebook /great_expectations/uncommitted/datasource_new.ipynb
      --allow-root --ip 0.0.0.0` and access jupyter notebook with one of the URL's given as result of the execution.

<p align="center">
   <img width="75%" alt="Datasource File Jupyter Notebook Command" src="datasource_Jupyter.png"/>
</p>
    3. Once you have accessed the file in Jupyter, you would have to fill out the information of your source. For example:

<p align="center">
   <img width="75%" alt="Datasource Credentials" src="datasource_Credentials.png"/>
</p>
    4. Last but not least, you would have to execute the whole notebook. During this execution you will be able to see if there is any error in your configuration;
        and in case there were no errors, this file will be automatically save.

**Suite File**:

