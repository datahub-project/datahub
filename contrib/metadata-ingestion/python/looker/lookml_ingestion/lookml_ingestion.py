import lkml
import glob
import time
import typing
import os
import re

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from dataclasses import dataclass, replace

from sql_metadata import get_query_tables

# Configuration
AVSC_PATH = "../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc"
KAFKA_TOPIC = 'MetadataChangeEvent_v4'

# LOOKER_DIRECTORY = "./test_lookml"
LOOKER_DIRECTORY = os.environ["LOOKER_DIRECTORY"]
LOOKER_DIRECTORY = os.path.abspath(LOOKER_DIRECTORY)

EXTRA_KAFKA_CONF = {
  'bootstrap.servers': 'localhost:9092',
  'schema.registry.url': 'http://localhost:8081'
  # 'security.protocol': 'SSL',
  # 'ssl.ca.location': '',
  # 'ssl.key.location': '',
  # 'ssl.certificate.location': ''
}

# The datahub platform where looker views are stored
LOOKER_VIEW_PLATFORM = "looker_views"

class LookerViewFileLoader:
	"""
	Loads the looker viewfile at a :path and caches the LookerViewFile in memory
	This is to avoid reloading the same file off of disk many times during the recursive include resolution process
	"""

	def __init__(self):
		self.viewfile_cache = {}

	def _load_viewfile(self, path: str) -> typing.Optional["LookerViewFile"]:
		if path in self.viewfile_cache:
			return self.viewfile_cache[path]

		try:
			with open(path, "r") as file:
				parsed = lkml.load(file)
				looker_viewfile = LookerViewFile.from_looker_dict(path, parsed)
				self.viewfile_cache[path] = looker_viewfile
				return looker_viewfile
		except Exception as e:
			print(e)
			print(f"Error processing view file {path}. Skipping it")

	def load_viewfile(self, path: str, connection: str):
		viewfile = self._load_viewfile(path)
		if viewfile is None:
			return None

		return replace(viewfile, connection=connection)



@dataclass
class LookerModel:
	connection: str
	includes: typing.List[str]
	resolved_includes: typing.List[str]

	@staticmethod
	def from_looker_dict(looker_model_dict):
		connection = looker_model_dict["connection"]
		includes = looker_model_dict["includes"]
		resolved_includes = LookerModel.resolve_includes(includes)

		return LookerModel(connection=connection, includes=includes, resolved_includes=resolved_includes)

	@staticmethod
	def resolve_includes(includes) -> typing.List[str]:
		resolved = []
		for inc in includes:
			# Massage the looker include into a valid glob wildcard expression
			glob_expr = f"{LOOKER_DIRECTORY}/{inc}"
			outputs = glob.glob(glob_expr)
			resolved.extend(outputs)
		return resolved


@dataclass
class LookerViewFile:
	absolute_file_path: str
	connection: typing.Optional[str]
	includes: typing.List[str]
	resolved_includes: typing.List[str]
	views: typing.List[typing.Dict]

	@staticmethod
	def from_looker_dict(absolute_file_path, looker_view_file_dict):
		includes = looker_view_file_dict.get("includes", [])
		resolved_includes = LookerModel.resolve_includes(includes)
		views = looker_view_file_dict.get("views", [])

		return LookerViewFile(absolute_file_path=absolute_file_path, connection=None, includes=includes, resolved_includes=resolved_includes, views=views)

@dataclass
class LookerView:
	absolute_file_path: str
	connection: str
	view_name: str
	sql_table_names: typing.List[str]

	def get_relative_file_path(self):
		if LOOKER_DIRECTORY in self.absolute_file_path:
			return self.absolute_file_path.replace(LOOKER_DIRECTORY, '').lstrip('/')
		else:
			raise Exception(f"Found a looker view with name: {view_name} at path: {absolute_file_path} not underneath the base LOOKER_DIRECTORY: {LOOKER_DIRECTORY}. This should not happen")

	@staticmethod
	def from_looker_dict(looker_view, connection: str, looker_viewfile: LookerViewFile, looker_viewfile_loader: LookerViewFileLoader) -> typing.Optional["LookerView"]:
		view_name = looker_view["name"]
		sql_table_name = looker_view.get("sql_table_name", None)
		# Some sql_table_name fields contain quotes like: optimizely."group", just remove the quotes
		sql_table_name = sql_table_name.replace('"', '') if sql_table_name is not None else None
		derived_table = looker_view.get("derived_table", None)

		# Parse SQL from derived tables to extract dependencies
		if derived_table is not None and 'sql' in derived_table:
			# Get the list of tables in the query
			sql_tables: typing.List[str] = get_query_tables(derived_table['sql'])

			# Remove temporary tables from WITH statements
			sql_table_names = [t for t in sql_tables if not re.search(f'WITH(.*,)?\s+{t}(\s*\([\w\s,]+\))?\s+AS\s+\(', derived_table['sql'], re.IGNORECASE|re.DOTALL)]

			# Remove quotes from tables
			sql_table_names = [t.replace('"', '') for t in sql_table_names]

			return LookerView(absolute_file_path=looker_viewfile.absolute_file_path, connection=connection, view_name=view_name, sql_table_names=sql_table_names)

		# There is a single dependency in the view, on the sql_table_name
		if sql_table_name is not None:
			return LookerView(absolute_file_path=looker_viewfile.absolute_file_path, connection=connection, view_name=view_name, sql_table_names=[sql_table_name])

		# The sql_table_name might be defined in another view and this view is extending that view, try to find it
		else:
			extends = looker_view.get("extends", [])
			if len(extends) == 0:
				# The view is malformed, the view is not a derived table, does not contain a sql_table_name or an extends
				print(f"Skipping malformed with view_name: {view_name}. View should have a sql_table_name if it is not a derived table")
				return None

			extends_to_looker_view = []

			# The base view could live in the same file
			for raw_view in looker_viewfile.views:
				raw_view_name = raw_view["name"]
				# Make sure to skip loading view we are currently trying to resolve
				if raw_view_name != view_name:
					maybe_looker_view = LookerView.from_looker_dict(raw_view, connection, looker_viewfile, looker_viewfile_loader)
					if maybe_looker_view is not None and maybe_looker_view.view_name in extends:
						extends_to_looker_view.append(maybe_looker_view)

			# Or it could live in one of the included files, we do not know which file the base view lives in, try them all!
			for include in looker_viewfile.resolved_includes:
				looker_viewfile = looker_viewfile_loader.load_viewfile(include, connection)
				if looker_viewfile is not None:
					for view in looker_viewfile.views:
						maybe_looker_view = LookerView.from_looker_dict(view, connection, looker_viewfile, looker_viewfile_loader)
						if maybe_looker_view is None:
							continue

						if maybe_looker_view is not None and maybe_looker_view.view_name in extends:
							extends_to_looker_view.append(maybe_looker_view)

			if len(extends_to_looker_view) != 1:
				print(f"Skipping malformed view with view_name: {view_name}. View should have a single view in a view inheritance chain with a sql_table_name")
				return None

			output_looker_view = LookerView(absolute_file_path=looker_viewfile.absolute_file_path, connection=connection, view_name=view_name, sql_table_names=extends_to_looker_view[0].sql_table_names)
			return output_looker_view




def get_platform_and_table(view_name: str, connection: str, sql_table_name: str):
	"""
	This will depend on what database connections you use in Looker
	For SpotHero, we had two database connections in Looker: "redshift_test" (a redshift database) and "presto" (a presto database)
	Presto supports querying across multiple catalogs, so we infer which underlying database presto is using based on the presto catalog name
	For SpotHero, we have 3 catalogs in presto: "redshift", "hive", and "hive_emr"
	"""
	if connection == "redshift_test":
		platform = "redshift"
		table_name = sql_table_name
		return platform, table_name

	elif connection == "presto":
		parts = sql_table_name.split(".")
		catalog = parts[0]

		if catalog == "hive":
			platform = "hive"
		elif catalog == "hive_emr":
			platform = "hive_emr"
		elif catalog == "redshift":
			platform = "redshift"
		else:
			# Looker lets you exclude a catalog and use a configured default, the default we have configured is to use hive_emr
			if sql_table_name.count(".") != 1:
				raise Exception("Unknown catalog for sql_table_name: {sql_table_name} for view_name: {view_name}")

			platform = "hive_emr"
			return platform, sql_table_name

		table_name = ".".join(parts[1::])
		return platform, table_name
	else:
		raise Exception(f"Could not find a platform for looker view with connection: {connection}")


def construct_datalineage_urn(view_name: str, connection: str, sql_table_name: str):
	platform, table_name = get_platform_and_table(view_name, connection, sql_table_name)
	return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{table_name},PROD)"

def construct_data_urn(looker_view: LookerView):
	return f"urn:li:dataset:(urn:li:dataPlatform:{LOOKER_VIEW_PLATFORM},{looker_view.view_name},PROD)"



def build_dataset_mce(looker_view: LookerView):
    """
    Creates MetadataChangeEvent for the dataset, creating upstream lineage links
    """
    actor, sys_time = "urn:li:corpuser:etl", int(time.time()) * 1000

    upstreams = [{
    	"auditStamp":{
    		"time": sys_time,
    		"actor":actor
    	},
    	"dataset": construct_datalineage_urn(looker_view.view_name, looker_view.connection, sql_table_name),
    	"type":"TRANSFORMED"
    } for sql_table_name in looker_view.sql_table_names]


    doc_elements = [{
    	"url":f"https://github.com/spothero/internal-looker-repo/blob/master/{looker_view.get_relative_file_path()}",
    	"description":"Github looker view definition",
    	"createStamp":{
    		"time": sys_time,
    		"actor": actor
    	}
    }]

    owners = [{
    	"owner": f"urn:li:corpuser:analysts",
    	"type": "DEVELOPER"
    }]

    return {
        "auditHeader": None,
        "proposedSnapshot":("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot", {
            "urn": construct_data_urn(looker_view),
            "aspects": [
            	("com.linkedin.pegasus2avro.dataset.UpstreamLineage", {"upstreams": upstreams}),
            	("com.linkedin.pegasus2avro.common.InstitutionalMemory", {"elements": doc_elements}),
            	("com.linkedin.pegasus2avro.common.Ownership", {
            		"owners": owners,
            		"lastModified":{
            			"time": sys_time,
            			"actor": actor
            		}
            	})
            ]
        }),
        "proposedDelta": None
    }


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


def main():
	kafka_producer = make_kafka_producer(EXTRA_KAFKA_CONF)
	viewfile_loader = LookerViewFileLoader()

	looker_models = []
	all_views = []

	model_files = sorted(f for f in glob.glob(f"{LOOKER_DIRECTORY}/**/*.model.lkml", recursive=True))
	for f in model_files:
		try:
			with open(f, 'r') as file:
				parsed = lkml.load(file)
				looker_model = LookerModel.from_looker_dict(parsed)
				looker_models.append(looker_model)
		except Exception as e:
			print(e)
			print(f"Error processing model file {f}. Skipping it")



	for model in looker_models:
		for include in model.resolved_includes:
			looker_viewfile = viewfile_loader.load_viewfile(include, model.connection)
			if looker_viewfile is not None:
				for raw_view in looker_viewfile.views:
					maybe_looker_view = LookerView.from_looker_dict(raw_view, model.connection, looker_viewfile, viewfile_loader)
					if maybe_looker_view:
						all_views.append(maybe_looker_view)


	for view in all_views:
		MCE = build_dataset_mce(view)
		print(view)
		print(MCE)
		kafka_producer.produce(topic=KAFKA_TOPIC, key=MCE['proposedSnapshot'][1]['urn'], value=MCE)
		kafka_producer.flush()



if __name__ == "__main__":
	main()
