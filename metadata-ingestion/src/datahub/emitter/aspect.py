from datahub.metadata.schema_classes import ASPECT_CLASSES

ASPECT_MAP = {
    AspectClass.get_aspect_name(): AspectClass for AspectClass in ASPECT_CLASSES
}

TIMESERIES_ASPECT_MAP = {
    name: klass
    for name, klass in ASPECT_MAP.items()
    if klass.get_aspect_type() == "timeseries"
}
