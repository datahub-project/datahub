from datahub.utilities.urns.urn import Urn


class OrganizationUrn(Urn):
    """
    URN for Organization entities in DataHub.
    
    Format: urn:li:organization:<organization_id>
    """

    ENTITY_TYPE = "organization"

    def __init__(self, organization_id: str):
        """
        Create an Organization URN.
        
        Args:
            organization_id: Unique identifier for the organization
        """
        super().__init__(self.ENTITY_TYPE, [organization_id])

    @classmethod
    def create_from_id(cls, organization_id: str) -> "OrganizationUrn":
        """
        Create an OrganizationUrn from an organization ID.
        
        Args:
            organization_id: Unique identifier for the organization
            
        Returns:
            OrganizationUrn instance
        """
        return cls(organization_id)

    @property
    def organization_id(self) -> str:
        """Get the organization ID from the URN."""
        return self.get_entity_id()[0]

    def __str__(self) -> str:
        return f"urn:li:{self.ENTITY_TYPE}:{self.organization_id}"
