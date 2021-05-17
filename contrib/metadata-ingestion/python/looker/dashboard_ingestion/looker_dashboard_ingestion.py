#! /usr/bin/python
import time
import os
import json
import typing
from pprint import pprint
import looker_sdk
from looker_sdk.sdk.api31.models import Query, DashboardElement, LookWithQuery, Dashboard
from looker_sdk.error import SDKError

from dataclasses import dataclass

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Configuration
AVSC_PATH = "../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc"
KAFKA_TOPIC = 'MetadataChangeEvent_v4'

# Set the following environmental variables to hit Looker's API
# LOOKERSDK_CLIENT_ID=YourClientID
# LOOKERSDK_CLIENT_SECRET=YourClientSecret
# LOOKERSDK_BASE_URL=https://company.looker.com:19999
LOOKERSDK_BASE_URL = os.environ["LOOKERSDK_BASE_URL"] 

EXTRA_KAFKA_CONF = {
	'bootstrap.servers': 'localhost:9092',
	'schema.registry.url': 'http://localhost:8081'
	# 'security.protocol': 'SSL',
	# 'ssl.ca.location': '',
	# 'ssl.key.location': '',
	# 'ssl.certificate.location': ''
}

# The datahub platform where looker views are stored, must be the same as VIEW_DATAHUB_PLATFORM in lookml_ingestion.py
VIEW_DATAHUB_PLATFORM = "looker_views"
# The datahub platform where looker dashboards will be stored
VISUALIZATION_DATAHUB_PLATFORM = "looker"


@dataclass
class LookerDashboardElement:
	id: str
	title: str
	query_slug: str
	looker_views: typing.List[str]
	look_id: typing.Optional[str]

	@property
	def url(self) -> str:
		base_url = get_looker_base_url()

		# A dashboard element can use a look or just a raw query against an explore
		if self.look_id is not None:
			return base_url + "/looks/" + self.look_id
		else:
			return base_url + "/x/" + self.query_slug

	def get_urn_element_id(self):
		# A dashboard element can use a look or just a raw query against an explore
		return f"dashboard_elements.{self.id}"

	def get_view_urns(self) -> typing.List[str]:
		return [f"urn:li:dataset:(urn:li:dataPlatform:{VIEW_DATAHUB_PLATFORM},{v},PROD)" for v in self.looker_views]


@dataclass
class LookerDashboard:
	id: str
	title: str
	description: str
	dashboard_elements: typing.List[LookerDashboardElement]

	@property
	def url(self):
		return get_looker_base_url() + "/dashboards/" + self.id

	def get_urn_dashboard_id(self):
		return f"dashboards.{self.id}"


@dataclass
class DashboardKafkaEvents:
	dashboard_mce: typing.Dict
	chart_mces: typing.List[typing.Dict]

	def all_mces(self) -> typing.List[typing.Dict]:
		return self.chart_mces + [self.dashboard_mce]


def get_looker_base_url():
	base_url = LOOKERSDK_BASE_URL.split("looker.com")[0] + "looker.com"
	return base_url


def get_actor_and_sys_time():
	actor, sys_time = "urn:li:corpuser:analysts", int(time.time()) * 1000
	return actor, sys_time


class ProperDatahubEvents:
	"""
	This class generates events for "proper" datahub charts and dashboards
	These events will not be visualized anywhere as of 12/11/2020
	"""
	@staticmethod
	def make_chart_mce(dashboard_element: LookerDashboardElement) -> typing.Dict:
		actor, sys_time = get_actor_and_sys_time()

		owners = [{
			"owner": actor,
			"type": "DEVELOPER"
		}]

		return {
			"auditHeader": None,
			"proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.ChartSnapshot", {
				"urn": f"urn:li:chart:(looker,{dashboard_element.get_urn_element_id()})",
				"aspects": [
					("com.linkedin.pegasus2avro.dataset.ChartInfo", {
						"title": dashboard_element.title,
						"description": "",
						"inputs": dashboard_element.get_view_urns(),
						"url": f"",
						"lastModified": {"created": {"time": sys_time, "actor": actor}}
					}),
					("com.linkedin.pegasus2avro.common.Ownership", {
						"owners": owners,
						"lastModified": {
							"time": sys_time,
							"actor": actor
						}
					})
				]
			}),
			"proposedDelta": None
		}

	@staticmethod
	def make_dashboard_mce(looker_dashboard: LookerDashboard) -> DashboardKafkaEvents:
		actor, sys_time = get_actor_and_sys_time()

		owners = [{
			"owner": actor,
			"type": "DEVELOPER"
		}]

		chart_mces = [ProperDatahubEvents.make_chart_mce(element) for element in looker_dashboard.dashboard_elements]

		dashboard_mce = {
			"auditHeader": None,
			"proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.DashboardSnapshot", {
				"urn": 	f"urn:li:dashboard:(looker,{looker_dashboard.get_urn_dashboard_id()},PROD)",
				"aspects": [
					("com.linkedin.pegasus2avro.dataset.DashboardInfo", {
						"title": looker_dashboard.title,
						"description": looker_dashboard.description,
						"charts": [mce["proposedSnapshot"][1]["urn"] for mce in chart_mces],
						"url": looker_dashboard.url,
						"lastModified": {"created": {"time": sys_time, "actor": actor}}
					}),
					("com.linkedin.pegasus2avro.common.Ownership", {
						"owners": owners,
						"lastModified": {
							"time": sys_time,
							"actor": actor
						}
					})
				]
			}),
			"proposedDelta": None
		}

		return DashboardKafkaEvents(dashboard_mce=dashboard_mce, chart_mces=chart_mces)


class WorkaroundDatahubEvents:
	"""
	This class generates events for "workaround" datahub charts and dashboards
	This is so we can display end to end lineage without being blocked on datahub's support for dashboards and charts

	The approach is we generate "charts" and "dashboards" as just "datasets" in datahub under a new platform
	We then link them together using "UpstreamLineage" just like any other dataset
	"""
	@staticmethod
	def make_chart_mce(dashboard_element: LookerDashboardElement) -> typing.Dict:
		actor, sys_time = get_actor_and_sys_time()

		owners = [{
			"owner": actor,
			"type": "DEVELOPER"
		}]

		upstreams = [{
			"auditStamp":{
				"time": sys_time,
				"actor": actor
			},
			"dataset": view_urn,
			"type":"TRANSFORMED"
		} for view_urn in dashboard_element.get_view_urns()]

		doc_elements = [{
			"url": dashboard_element.url,
			"description": "Looker chart url",
			"createStamp": {
				"time": sys_time,
				"actor": actor
			}
		}]

		return {
			"auditHeader": None,
			"proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot", {
				"urn": f"urn:li:dataset:(urn:li:dataPlatform:{VISUALIZATION_DATAHUB_PLATFORM},{dashboard_element.get_urn_element_id()},PROD)",
				"aspects": [
					("com.linkedin.pegasus2avro.dataset.UpstreamLineage", {"upstreams": upstreams}),
					("com.linkedin.pegasus2avro.common.InstitutionalMemory", {"elements": doc_elements}),
					("com.linkedin.pegasus2avro.dataset.DatasetProperties", {"description": dashboard_element.title, "customProperties": {}}),
					("com.linkedin.pegasus2avro.common.Ownership", {
						"owners": owners,
						"lastModified": {
							"time": sys_time,
							"actor": actor
						}
					})
				]
			}),
			"proposedDelta": None
		}

	@staticmethod
	def make_dashboard_mce(looker_dashboard: LookerDashboard) -> DashboardKafkaEvents:
		actor, sys_time = get_actor_and_sys_time()

		chart_mces = [WorkaroundDatahubEvents.make_chart_mce(element) for element in looker_dashboard.dashboard_elements]

		owners = [{
			"owner": actor,
			"type": "DEVELOPER"
		}]

		upstreams = [{
			"auditStamp":{
				"time": sys_time,
				"actor": actor
			},
			"dataset": chart_urn,
			"type":"TRANSFORMED"
		} for chart_urn in [mce["proposedSnapshot"][1]["urn"] for mce in chart_mces]]

		doc_elements = [{
			"url": looker_dashboard.url,
			"description": "Looker dashboard url",
			"createStamp": {
				"time": sys_time,
				"actor": actor
			}
		}]

		dashboard_mce = {
			"auditHeader": None,
			"proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot", {
				"urn": f"urn:li:dataset:(urn:li:dataPlatform:{VISUALIZATION_DATAHUB_PLATFORM},{looker_dashboard.get_urn_dashboard_id()},PROD)",
				"aspects": [
					("com.linkedin.pegasus2avro.dataset.UpstreamLineage", {"upstreams": upstreams}),
					("com.linkedin.pegasus2avro.common.InstitutionalMemory", {"elements": doc_elements}),
					("com.linkedin.pegasus2avro.dataset.DatasetProperties", {"description": looker_dashboard.title, "customProperties": {}}),
					("com.linkedin.pegasus2avro.common.Ownership", {
						"owners": owners,
						"lastModified": {
							"time": sys_time,
							"actor": actor
						}
					})
				]
			}),
			"proposedDelta": None
		}

		return DashboardKafkaEvents(dashboard_mce=dashboard_mce, chart_mces=chart_mces)


def delivery_report(err, msg):
	""" Called once for each message produced to indicate delivery result.
		Triggered by poll() or flush(). """
	if err is not None:
		print('Message delivery failed: {}'.format(err))
	else:
		print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def make_kafka_producer(extra_kafka_conf):
	conf = {
		"on_delivery": delivery_report,
		**extra_kafka_conf
	}

	key_schema = avro.loads('{"type": "string"}')
	record_schema = avro.load(AVSC_PATH)
	producer = AvroProducer(conf, default_key_schema=key_schema, default_value_schema=record_schema)
	return producer


def _extract_view_from_field(field: str) -> str:
	assert field.count(".") == 1, f"Error: A field must be prefixed by a view name, field is: {field}"
	view_name = field.split(".")[0]
	return view_name


def get_views_from_query(query: Query) -> typing.List[str]:
	all_views = set()

	# query.dynamic_fields can contain:
	# - looker table calculations: https://docs.looker.com/exploring-data/using-table-calculations
	# - looker custom measures: https://docs.looker.com/de/exploring-data/adding-fields/custom-measure
	# - looker custom dimensions: https://docs.looker.com/exploring-data/adding-fields/custom-measure#creating_a_custom_dimension_using_a_looker_expression
	dynamic_fields = json.loads(query.dynamic_fields if query.dynamic_fields is not None else '[]')
	custom_field_to_underlying_field = {}
	for field in dynamic_fields:
		# Table calculations can only reference fields used in the fields section, so this will always be a subset of of the query.fields
		if "table_calculation" in field:
			continue
		# Looker custom measures can reference fields in arbitrary views, so this needs to be parsed to find the underlying view field the custom measure is based on
		if "measure" in field:
			measure = field["measure"]
			based_on = field["based_on"]
			custom_field_to_underlying_field[measure] = based_on

		# Looker custom dimensions can reference fields in arbitrary views, so this needs to be parsed to find the underlying view field the custom measure is based on
		# However, unlike custom measures custom dimensions can be defined using an arbitrary expression
		# We are not going to support parsing arbitrary Looker expressions here, so going to ignore these fields for now
		# TODO: support parsing arbitrary looker expressions
		if "dimension" in field:
			dimension = field["dimension"]
			expression = field["expression"]
			custom_field_to_underlying_field[dimension] = None

	# A query uses fields defined in views, find the views those fields use
	fields: typing.Sequence[str] = query.fields if query.fields is not None else []
	for field in fields:
		# If the field is a custom field, look up the field it is based on
		field_name = custom_field_to_underlying_field[field] if field in custom_field_to_underlying_field else field
		if field_name is None:
			continue
		view_name = _extract_view_from_field(field_name)
		all_views.add(view_name)

	# A query uses fields for filtering and those fields are defined in views, find the views those fields use
	filters: typing.MutableMapping[str, typing.Any] = query.filters if query.filters is not None else {}
	for field in filters.keys():
		# If the field is a custom field, look up the field it is based on
		field_name = custom_field_to_underlying_field[field] if field in custom_field_to_underlying_field else field
		if field_name is None:
			continue
		view_name = _extract_view_from_field(field_name)
		all_views.add(view_name)

	return list(all_views)


def get_views_from_look(look: LookWithQuery):
	return get_views_from_query(look.query)


def get_looker_dashboard_element(element: DashboardElement)-> typing.Optional[LookerDashboardElement]:
	# Dashboard elements can use raw queries against explores
	if element.query is not None:
		views = get_views_from_query(element.query)
		return LookerDashboardElement(id=element.id, title=element.title, look_id=None, query_slug=element.query.slug, looker_views=views)

	# Dashboard elements can *alternatively* link to an existing look
	if element.look is not None:
		views = get_views_from_look(element.look)
		return LookerDashboardElement(id=element.id, title=element.title, look_id=element.look_id, query_slug=element.look.query.slug, looker_views=views)

	# This occurs for "text" dashboard elements that just contain static text (ie: no queries)
	# There is not much meaningful info to extract from these elements, so ignore them
	return None


def get_looker_dashboard(dashboard: Dashboard) -> LookerDashboard:
	dashboard_elements: typing.List[LookerDashboardElement] = []
	for element in dashboard.dashboard_elements:
		looker_dashboard_element = get_looker_dashboard_element(element)
		if looker_dashboard_element is not None:
			dashboard_elements.append(looker_dashboard_element)

	looker_dashboard = LookerDashboard(id=dashboard.id, title=dashboard.title, description=dashboard.description, dashboard_elements=dashboard_elements)
	return looker_dashboard


# Perform IO in main
def main():
	kafka_producer = make_kafka_producer(EXTRA_KAFKA_CONF)
	sdk = looker_sdk.init31()
	dashboard_ids = [dashboard_base.id for dashboard_base in sdk.all_dashboards(fields="id")]

	looker_dashboards = []
	for dashboard_id in dashboard_ids:
		try:
			fields = ["id", "title", "dashboard_elements", "dashboard_filters"]
			dashboard_object = sdk.dashboard(dashboard_id=dashboard_id, fields=",".join(fields))
		except SDKError as e:
			# A looker dashboard could be deleted in between the list and the get
			print(f"Skipping dashboard with dashboard_id: {dashboard_id}")
			print(e)
			continue

		looker_dashboard = get_looker_dashboard(dashboard_object)
		looker_dashboards.append(looker_dashboard)
		pprint(looker_dashboard)

	for looker_dashboard in looker_dashboards:
		workaround_dashboard_kafka_events = WorkaroundDatahubEvents.make_dashboard_mce(looker_dashboard)
		# Hard to test these events since datahub does not have a UI, for now disable sending them
		# proper_dashboard_kafka_events = ProperDatahubEvents.make_dashboard_mce(looker_dashboard)

		for mce in workaround_dashboard_kafka_events.all_mces():
			print(mce)
			kafka_producer.produce(topic=KAFKA_TOPIC, key=mce['proposedSnapshot'][1]['urn'], value=mce)
			kafka_producer.flush()


if __name__ == "__main__":
	main()
