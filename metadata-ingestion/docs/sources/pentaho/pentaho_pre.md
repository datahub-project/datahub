### Overview

The `pentaho` module ingests Pentaho Data Integration (Kettle) files into DataHub. It parses every `.ktr` and `.kjb` file under a folder and emits each as a DataJob with table-level lineage.

### Prerequisites

This connector reads files from a filesystem path. It does not connect to a Pentaho server, repository, or database, so it needs no credentials, only read access to the folder.

Install it with `pip install 'acryl-datahub[pentaho]'`. The extra provides the SQL parser used for `TableInput` lineage and the XML library used to parse Kettle files.

#### Steps to Get the Required Information

1. Locate or export the directory containing your `.ktr` and `.kjb` files.
2. Set `base_folder` to that path. Subdirectories are scanned recursively.
3. Set `platform_mappings` if your connection types are not covered by the defaults.
