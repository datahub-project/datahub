from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import GlossaryRelatedTermsClass
from datahub.metadata.urns import GlossaryTermUrn

# First, ensure the related terms exist (you would have created these previously)
# For this example, assume we have:
# - Classification.PII (a broad category)
# - Classification.Sensitive (another category)
# - PersonalInformation.Email (a specific term)
# - PersonalInformation.Address (another specific term)

# Create relationships for the Email term
email_term_urn = make_term_urn("PersonalInformation.Email")

# Define relationships
email_relationships = GlossaryRelatedTermsClass(
    # IsA relationship: Email is a type of PII
    # This creates an inheritance hierarchy
    isRelatedTerms=[
        str(GlossaryTermUrn("Classification.PII")),
        str(GlossaryTermUrn("Classification.Sensitive")),
    ],
    # RelatedTo: General semantic relationship
    relatedTerms=[
        str(GlossaryTermUrn("PersonalInformation.PhoneNumber")),
        str(GlossaryTermUrn("PersonalInformation.Contact")),
    ],
)

# Create relationships for the Address term
address_term_urn = make_term_urn("PersonalInformation.Address")

address_relationships = GlossaryRelatedTermsClass(
    # IsA: Address is also a type of PII
    isRelatedTerms=[str(GlossaryTermUrn("Classification.PII"))],
    # HasA: Address contains these components
    hasRelatedTerms=[
        str(GlossaryTermUrn("PersonalInformation.ZipCode")),
        str(GlossaryTermUrn("PersonalInformation.Street")),
        str(GlossaryTermUrn("PersonalInformation.City")),
        str(GlossaryTermUrn("PersonalInformation.Country")),
    ],
)

# Create an enumeration term with fixed values
color_enum_urn = make_term_urn("ColorEnum")

color_enum_relationships = GlossaryRelatedTermsClass(
    # Values: Define the allowed values for this enumeration
    values=[
        str(GlossaryTermUrn("Colors.Red")),
        str(GlossaryTermUrn("Colors.Green")),
        str(GlossaryTermUrn("Colors.Blue")),
        str(GlossaryTermUrn("Colors.Yellow")),
    ]
)

# Emit the relationships
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Emit Email term relationships
rest_emitter.emit(
    MetadataChangeProposalWrapper(entityUrn=email_term_urn, aspect=email_relationships)
)
print(f"Added relationships to: {email_term_urn}")

# Emit Address term relationships
rest_emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=address_term_urn, aspect=address_relationships
    )
)
print(f"Added relationships to: {address_term_urn}")

# Emit Color enumeration relationships
rest_emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=color_enum_urn, aspect=color_enum_relationships
    )
)
print(f"Added value relationships to: {color_enum_urn}")

print("\nRelationship types explained:")
print("- isRelatedTerms (IsA): Inheritance relationship - term is a type of another")
print("- hasRelatedTerms (HasA): Containment relationship - term contains other terms")
print("- values: Enumeration values - defines allowed values for the term")
print("- relatedTerms: General semantic relationship between terms")
