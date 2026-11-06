section Section1;
shared Policies = let
    Source = Snowflake.Databases("my-account.snowflakecomputing.com","COMPUTE_WH"),
    SALES_DB = Source{[Name="SALES_DB",Kind="Database"]}[Data],
    PUBLIC = SALES_DB{[Name="PUBLIC",Kind="Schema"]}[Data],
    POLICIES = PUBLIC{[Name="POLICIES",Kind="Table"]}[Data]
in
    POLICIES;
// Helper query: not loaded, so it is not an entity of the dataflow.
Agents_Staging = let
    Source = Sql.Database("sql-host.example.com", "Agency"),
    dbo_Agents = Source{[Schema="dbo",Item="Agents"]}[Data]
in
    dbo_Agents;
[StagingDefinition = [Kind = "FastCopy"]]
shared #"Active Agents" = let
    Source = Agents_Staging,
    Active = Table.SelectRows(Source, each [Status] = "Active; Licensed")
in
    Active;
