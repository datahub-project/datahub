# SHACL Migration Guide

## Overview

This guide helps developers migrate from the legacy SKOS approach to the modern SHACL approach for dataset field definitions. Both approaches are supported, but SHACL provides richer constraint modeling and validation capabilities.

## When to Migrate

### **Keep SKOS Approach For:**

- Simple field definitions
- Basic descriptions
- No validation requirements
- Quick prototyping
- Reference data fields

### **Migrate to SHACL Approach For:**

- Fields requiring constraints (`maxLength`, `minCount`, etc.)
- Validation rules
- Complex business logic
- Financial calculations
- Regulatory compliance fields
- Fields with SQL-specific metadata

## Migration Steps

### Step 1: Identify Fields to Migrate

Look for fields that would benefit from constraints:

```turtle
# Before: Simple field (keep SKOS)
<http://COUNTERPARTY_MASTER/Counterparty_Master#legal_name> a schema:PropertyValue ;
    schema:name "LEGAL_NM" ;
    schema:description "Legal name of the counterparty entity" ;
    schema:unitText "VARCHAR(200)" ;
    skos:exactMatch counterparty:Legal_Name .

# After: Complex field (migrate to SHACL)
accounts:accountIdProperty a sh:PropertyShape ;
    sh:path accounts:accountId ;
    sh:class accounts:Account_ID ;
    sh:datatype xsd:string ;
    sh:maxLength 20 ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
    sh:name "Account ID" ;
    sh:description "Unique identifier for the account" ;
    ex:sqlType "VARCHAR(20)" ;
    ex:validationRule "Must be unique across all accounts" .
```

### Step 2: Create Property Shapes

Define reusable `sh:PropertyShape` instances:

```turtle
# Define property shapes
accounts:accountIdProperty a sh:PropertyShape ;
    sh:path accounts:accountId ;
    sh:class accounts:Account_ID ;
    sh:datatype xsd:string ;
    sh:maxLength 20 ;
    sh:name "Account ID" ;
    sh:description "Unique identifier for the account" ;
    ex:sqlType "VARCHAR(20)" .

accounts:riskWeightProperty a sh:PropertyShape ;
    sh:path accounts:riskWeight ;
    sh:class accounts:Risk_Weight ;
    sh:datatype xsd:decimal ;
    sh:minInclusive 0.0 ;
    sh:maxInclusive 1.0 ;
    sh:name "Risk Weight" ;
    sh:description "Risk weight percentage for capital adequacy calculation" ;
    ex:sqlType "DECIMAL(5,2)" .
```

### Step 3: Create Node Shape

Define the dataset schema using `sh:NodeShape`:

```turtle
<http://DataHubFinancial.com/REFERENCE_DATA/ACCOUNTS/Account_Details_Schema> a sh:NodeShape ;
    sh:targetClass <http://DataHubFinancial.com/REFERENCE_DATA/ACCOUNTS/AccountRecord> ;
    rdfs:label "Account Master Schema" ;
    rdfs:comment "Schema for account master data records" ;
    sh:property [
        sh:node accounts:accountIdProperty ;
        sh:minCount 1 ;
        sh:maxCount 1
    ] ;
    sh:property [
        sh:node accounts:riskWeightProperty ;
        sh:minCount 1 ;
        sh:maxCount 1
    ] .
```

### Step 4: Link Dataset to Schema

Connect the dataset to its schema:

```turtle
<http://DataHubFinancial.com/REFERENCE_DATA/ACCOUNTS/Account_Details> a dcat:Dataset ;
    dcterms:title "Account Master" ;
    dcterms:description "Master reference data for account-level information" ;
    dcterms:conformsTo <http://DataHubFinancial.com/REFERENCE_DATA/ACCOUNTS/Account_Details_Schema> .
```

## Property Shape Properties

### **Core Properties**

| Property         | Description             | Example                               |
| ---------------- | ----------------------- | ------------------------------------- |
| `sh:path`        | Field path/identifier   | `accounts:accountId`                  |
| `sh:class`       | Glossary term reference | `accounts:Account_ID`                 |
| `sh:datatype`    | XSD datatype            | `xsd:string`, `xsd:decimal`           |
| `sh:name`        | Human-readable name     | `"Account ID"`                        |
| `sh:description` | Field description       | `"Unique identifier for the account"` |

### **Constraint Properties**

| Property          | Description              | Example                           |
| ----------------- | ------------------------ | --------------------------------- |
| `sh:minLength`    | Minimum string length    | `sh:minLength 1`                  |
| `sh:maxLength`    | Maximum string length    | `sh:maxLength 20`                 |
| `sh:minCount`     | Minimum occurrence count | `sh:minCount 1`                   |
| `sh:maxCount`     | Maximum occurrence count | `sh:maxCount 1`                   |
| `sh:minInclusive` | Minimum numeric value    | `sh:minInclusive 0.0`             |
| `sh:maxInclusive` | Maximum numeric value    | `sh:maxInclusive 1.0`             |
| `sh:pattern`      | Regex pattern            | `sh:pattern "^[A-Z]{2}[0-9]{6}$"` |

### **Custom Properties**

| Property            | Description                | Example                                                 |
| ------------------- | -------------------------- | ------------------------------------------------------- |
| `ex:sqlType`        | SQL-specific type          | `ex:sqlType "VARCHAR(20)"`                              |
| `ex:validationRule` | Business validation rule   | `ex:validationRule "Must be unique"`                    |
| `ex:businessRule`   | Business logic description | `ex:businessRule "Risk weight must be between 0 and 1"` |

## Datatype Mapping

| XSD Datatype   | DataHub Type       | SQL Type       |
| -------------- | ------------------ | -------------- |
| `xsd:string`   | `StringTypeClass`  | `VARCHAR(n)`   |
| `xsd:decimal`  | `NumberTypeClass`  | `DECIMAL(p,s)` |
| `xsd:integer`  | `NumberTypeClass`  | `INTEGER`      |
| `xsd:date`     | `DateTypeClass`    | `DATE`         |
| `xsd:dateTime` | `DateTypeClass`    | `TIMESTAMP`    |
| `xsd:boolean`  | `BooleanTypeClass` | `BOOLEAN`      |

## Migration Checklist

### **Before Migration**

- [ ] Identify fields that need constraints
- [ ] Review existing glossary terms
- [ ] Plan property shape organization
- [ ] Test with sample data

### **During Migration**

- [ ] Create property shapes for complex fields
- [ ] Define node shape for dataset schema
- [ ] Link dataset to schema via `dcterms:conformsTo`
- [ ] Test field-to-concept mapping
- [ ] Verify constraint validation

### **After Migration**

- [ ] Test complete pipeline
- [ ] Verify DataHub integration
- [ ] Update documentation
- [ ] Train team on new approach

## Examples

### **Simple Field (Keep SKOS)**

```turtle
# Reference data - no constraints needed
<http://COUNTERPARTY_MASTER/Counterparty_Master#legal_name> a schema:PropertyValue ;
    schema:name "LEGAL_NM" ;
    schema:description "Legal name of the counterparty entity" ;
    schema:unitText "VARCHAR(200)" ;
    skos:exactMatch counterparty:Legal_Name .
```

### **Complex Field (Migrate to SHACL)**

```turtle
# Financial calculation - needs constraints
accounts:riskWeightProperty a sh:PropertyShape ;
    sh:path accounts:riskWeight ;
    sh:class accounts:Risk_Weight ;
    sh:datatype xsd:decimal ;
    sh:minInclusive 0.0 ;
    sh:maxInclusive 1.0 ;
    sh:name "Risk Weight" ;
    sh:description "Risk weight percentage for capital adequacy calculation" ;
    ex:sqlType "DECIMAL(5,2)" ;
    ex:validationRule "Must be between 0 and 1 for regulatory compliance" .
```

## Troubleshooting

### **Common Issues**

1. **Field not mapping to glossary term**

   - Check `sh:class` references valid glossary term URI
   - Verify glossary term is defined as `skos:Concept`

2. **Constraints not working**

   - Ensure XSD datatypes are properly prefixed
   - Check constraint values are valid for datatype

3. **Schema not loading**
   - Verify `dcterms:conformsTo` points to valid `sh:NodeShape`
   - Check all `sh:node` references point to valid `sh:PropertyShape`

### **Validation**

Test your migration with:

```bash
# Test field-to-concept mapping
python -m rdf --source your_file.ttl --export-only datasets glossary --dry-run

# Check for parsing errors
python -m rdf --source your_file.ttl --validate-only
```

## Best Practices

1. **Start Small**: Migrate one dataset at a time
2. **Test Thoroughly**: Verify field-to-concept mapping works
3. **Document Changes**: Update team documentation
4. **Use Constraints Wisely**: Only add constraints that add value
5. **Maintain Consistency**: Use consistent naming patterns
6. **Reuse Property Shapes**: Define once, use multiple times

## Support

For questions or issues with SHACL migration:

- Check the [RDF Dataset Mapping Reference](RDF_DATASET_MAPPING.md)
- Review the [RDF Glossary Mapping Reference](RDF_GLOSSARY_MAPPING.md)
- Test with the dry-run mode before production use
