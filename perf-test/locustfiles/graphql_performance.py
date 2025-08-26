from locust import HttpUser, constant, task
from threading import Lock, Thread
from random import randint

from test_utils.datahub_sessions import DataHubSessions
from test_utils.graphql_queries import GraphQLQueries
from test_utils.test_suites import LINEAGE

lock = Lock()


datahub_instances = DataHubSessions()
graphql_queries = GraphQLQueries()
test_inputs = LINEAGE


class SearchUser(HttpUser):
    wait_time = constant(1)

    @task
    def search(self):
        test_run = test_inputs[randint(0, len(test_inputs)-1)]
        session = datahub_instances.get_session(self.host)
        gql_query = graphql_queries.get_query(test_run["query_name"])

        if "inputs" in test_run:
            inputs = test_run["inputs"]
            rand_input = inputs[randint(0, len(inputs)-1)]
            test_name = rand_input["test_name"]
            post_json = gql_query.get_query_with_inputs(rand_input)
        elif "variables" in test_run:
            variables = test_run["variables"]
            rand_vars = variables[randint(0, len(variables)-1)]
            test_name = rand_vars["test_name"]
            post_json = gql_query.get_query_with_variables(rand_vars)
        else:
            raise "Unknown test run type: {}".format(test_run)

        response = self.client.request(
            method="POST",
            name=f"{session.get_short_host()}[{test_name}]",
            url=f"{self.host}/api/graphql",
            cookies=session.get_cookies(),
            json=post_json
        )
        response.raise_for_status()
