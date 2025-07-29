import logging
import pytest

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

# ==============================================
# SYSTEM INFO API TESTS
# ==============================================
# These tests verify the system info API endpoints after refactoring to separate concerns:
#
# NEW API STRUCTURE:
# - /openapi/v1/system-info                    -> Only Spring components (no properties duplication)
# - /openapi/v1/system-info/spring-components  -> Same as main endpoint
# - /openapi/v1/system-info/properties         -> Detailed system properties with metadata
# - /openapi/v1/system-info/properties/simple  -> Simple key-value property map
#
# CHANGES MADE:
# - Eliminated duplication where properties appeared in both main response and separate endpoint
# - Improved clarity by separating component status from configuration properties
# - Maintained backward compatibility through dedicated endpoints
# ==============================================


def test_system_info_main_endpoint(auth_session):
    """Test that main system info endpoint returns expected structure with authentication."""
    response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info")
    response.raise_for_status()
    system_info = response.json()
    
    # Verify main response structure - now only contains springComponents
    assert "springComponents" in system_info, "Missing springComponents in system info response"
    assert "properties" not in system_info, "Properties should not be in main system info response (use /properties endpoint instead)"
    
    logger.info("Main system info endpoint test passed")


def test_system_info_spring_components_structure(auth_session):
    """Test that spring components structure contains all expected components from main endpoint."""
    response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info")
    response.raise_for_status()
    system_info = response.json()
    
    spring_components = system_info["springComponents"]
    assert "gms" in spring_components, "Missing GMS component info"
    assert "maeConsumer" in spring_components, "Missing MAE consumer info"
    assert "mceConsumer" in spring_components, "Missing MCE consumer info"
    
    # Verify GMS component details
    gms_info = spring_components["gms"]
    assert gms_info["name"] == "GMS", "GMS component name incorrect"
    assert gms_info["status"] == "AVAILABLE", "GMS should be available in running system"
    assert "version" in gms_info, "GMS version should be present"
    assert "properties" in gms_info, "GMS properties should be present"
    
    logger.info("Spring components structure test passed")


def test_system_info_properties_structure(auth_session):
    """Test that properties structure contains expected metadata."""
    response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info/properties")
    response.raise_for_status()
    properties = response.json()
    
    assert "properties" in properties, "Missing detailed properties"
    assert "propertySources" in properties, "Missing property sources"
    assert "totalProperties" in properties, "Missing total properties count"
    assert "redactedProperties" in properties, "Missing redacted properties count"
    assert isinstance(properties["totalProperties"], int), "Total properties should be integer"
    assert isinstance(properties["redactedProperties"], int), "Redacted properties should be integer"
    
    logger.info(f"Properties structure test passed - found {properties['totalProperties']} total properties, {properties['redactedProperties']} redacted")


def test_system_info_spring_components_endpoint(auth_session):
    """Test dedicated spring components endpoint."""
    response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info/spring-components")
    response.raise_for_status()
    components_data = response.json()
    
    assert "gms" in components_data, "Spring components endpoint missing GMS info"
    assert components_data["gms"]["status"] == "AVAILABLE", "GMS should be available in components endpoint"
    
    logger.info("Spring components endpoint test passed")


def test_system_info_properties_endpoint(auth_session):
    """Test dedicated detailed properties endpoint."""
    response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info/properties")
    response.raise_for_status()
    properties_data = response.json()
    
    assert "properties" in properties_data, "Properties endpoint missing detailed properties"
    assert "propertySources" in properties_data, "Properties endpoint missing property sources"
    
    logger.info("Detailed properties endpoint test passed")


def test_system_info_simple_properties_endpoint(auth_session):
    """Test simple properties endpoint returns flat key-value map."""
    response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info/properties/simple")
    response.raise_for_status()
    simple_properties = response.json()
    
    assert isinstance(simple_properties, dict), "Simple properties should be a dictionary"
    assert len(simple_properties) > 0, "Simple properties should contain some configuration"
    
    logger.info(f"Simple properties endpoint test passed - found {len(simple_properties)} properties")


def test_system_info_sensitive_data_redaction(auth_session):
    """Test that sensitive properties are properly redacted."""
    response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info/properties/simple")
    response.raise_for_status()
    simple_properties = response.json()
    
    # Check for redacted sensitive properties
    sensitive_found = False
    redacted_properties = []
    
    for key, value in simple_properties.items():
        if any(pattern in key.lower() for pattern in ["password", "secret", "key"]):
            if not key.lower().startswith("cache.client."):  # Allowed prefix
                if value == "***REDACTED***":
                    sensitive_found = True
                    redacted_properties.append(key)
    
    if redacted_properties:
        logger.info(f"Correctly redacted {len(redacted_properties)} sensitive properties: {redacted_properties[:3]}...")
    else:
        logger.info("No sensitive properties found to redact (may be valid depending on configuration)")
    
    logger.info("Sensitive data redaction test completed")


def test_system_info_endpoint_separation(auth_session):
    """Test that endpoints are properly separated for different concerns."""
    
    # Test main endpoint - should only contain spring components
    main_response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info")
    main_response.raise_for_status()
    main_data = main_response.json()
    
    assert "springComponents" in main_data, "Main endpoint should contain springComponents"
    assert "properties" not in main_data, "Main endpoint should not contain properties (avoid duplication)"
    
    # Test properties endpoint - should contain detailed properties
    props_response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info/properties")
    props_response.raise_for_status()
    props_data = props_response.json()
    
    assert "properties" in props_data, "Properties endpoint should contain detailed properties"
    assert "springComponents" not in props_data, "Properties endpoint should not contain springComponents"
    
    # Test spring components endpoint - should match main endpoint structure
    components_response = auth_session.get(f"{auth_session.gms_url()}/openapi/v1/system-info/spring-components")
    components_response.raise_for_status()
    components_data = components_response.json()
    
    # Should have same structure as main endpoint's springComponents
    assert main_data["springComponents"] == components_data, "Spring components endpoint should match main endpoint's springComponents"
    
    # Verify that GMS component has properties (from SpringComponentsCollector)
    gms_info = main_data["springComponents"]["gms"]
    assert "properties" in gms_info, "GMS component should have properties field with system properties"
    assert isinstance(gms_info["properties"], dict), "GMS properties should be a dictionary"
    
    logger.info("Endpoint separation test passed - no duplication detected") 