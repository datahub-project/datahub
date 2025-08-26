from kubernetes import client, config
from kubernetes.client.rest import ApiException
import requests
import base64
from urllib.parse import urlparse
import functools


class DataHubSessions:

    def __init__(self):
        self._session_cache = {}

    def get_session(self, url):
        if url not in self._session_cache:
            self._session_cache[url] = DataHubSession(url)

        return self._session_cache[url]


class DataHubSession:
    """
    Builds a map of url to k8 context & namespace. Extracts k8 secret to login with requests for
    authentication cookies.
    """

    def __init__(self, url, username='admin'):
        self._url = url
        self._host = urlparse(url).netloc
        try:
            context, namespace, secret = DataHubSession._find_credentials(self._host)
            self._session = self._get_session(username, secret)
        except:
            raise RuntimeError("Unable to locate credentials")

    def get_url(self):
        return self._url

    def get_short_host(self):
        return self._host.replace(".acryl.io", "")

    def get_cookies(self):
        return self._session.cookies.get_dict()

    @staticmethod
    def _get_filtered_contexts():
        def filter(context_name):
            if context_name.startswith("gke_acryl"):
                return True
            if context_name.startswith("arn:aws:eks:") and '795586375822' in context_name:
                return True
            return False

        contexts, active_context = config.list_kube_config_contexts()
        return [context for context in contexts if filter(context['name'])]

    @functools.cache
    @staticmethod
    def _find_credentials(target_host):
        short_host = target_host.replace(".acryl.io", "")

        def namespace_search(ctx, optimistic=True):
            """
            :param optimistic: assume hostname in namespace name
            """
            namespaces = DataHubSession._get_namespaces(ctx['name'])
            print(f"Searching k8 context: {ctx['name']} optimistic:{optimistic}")
            for namespace in [n for n in namespaces if not optimistic or short_host in n.metadata.name]:
                print(f"Searching namespace: {namespace.metadata.name} for ingress {target_host}")
                spec = DataHubSession._get_ingress_spec(ctx['name'], namespace.metadata.name)
                if spec:
                    for rule in spec.rules:
                        if rule.host == target_host:
                            print("Found namespace: " + namespace.metadata.name)
                            return namespace.metadata.name
            return None

        for optimistic in [True, False]:
            for context in DataHubSession._get_filtered_contexts():
                matched_namespace = namespace_search(context, optimistic=optimistic)
                if matched_namespace:
                    return context, matched_namespace, DataHubSession._get_secret(context['name'], matched_namespace)

        return None

    @functools.cache
    @staticmethod
    def _get_namespaces(context_name):
        k8_v1 = client.CoreV1Api(api_client=config.new_client_from_config(context=context_name))
        return k8_v1.list_namespace().items

    @functools.cache
    @staticmethod
    def _get_ingress_spec(context_name, namespace, default_ingress='dh-acryl-api-gateway'):
        k8_net_v1 = client.NetworkingV1Api(api_client=config.new_client_from_config(context=context_name))

        for ingress_name in [default_ingress, f"{namespace}-ingress"]:
            try:
                return k8_net_v1.read_namespaced_ingress(ingress_name, namespace).spec
            except ApiException as e:
                if e.status == 404:
                    print(f"Ingress {ingress_name} not found in {namespace}")
        return None

    @staticmethod
    def _get_secret(context_name, namespace, secret_name='datahub-secrets', secret_key='datahub-password'):
        k8_v1 = client.CoreV1Api(api_client=config.new_client_from_config(context=context_name))
        secret = k8_v1.read_namespaced_secret(secret_name, namespace)
        return base64.b64decode(secret.data[secret_key]).decode('utf-8')

    def _get_session(self, username, password):
        session = requests.Session()
        headers = {
            "Content-Type": "application/json",
        }
        data = '{"username":"' + username + '", "password":"' + password + '"}'
        response = session.post(f"{self._url}/logIn", headers=headers, data=data)
        response.raise_for_status()

        return session


if __name__ == "__main__":
    session_cache = DataHubSessions()
    session = session_cache.get_session("https://staging.acryl.io")
    print(session.get_cookies())
    session = session_cache.get_session("https://staging-test.acryl.io")
    print(session.get_cookies())
