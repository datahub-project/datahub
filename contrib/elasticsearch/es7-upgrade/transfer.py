# Copies indices (settings, mappings, and optionally data) from a 5 cluster to a 7 cluster.
# Note that when copying data, the copied is performed through this machine, meaning all data is downloaded from 5,
# and then uploaded to 7. This can be a very slow process if you have a lot of data, and is recommended you only do
# this for small indices as a result.

# Requires python 3+ and elasticsearch's python lib to be installed (pip install elasticsearch).

import argparse
import elasticsearch
import elasticsearch.helpers
import ssl
import time

parser = argparse.ArgumentParser(description="Transfers ES indexes between clusters.")
parser.add_argument('-s', '--source', required=True, help='Source cluster URL and port.')
parser.add_argument('-d', '--dest', required=True, help='Destination cluster URL and port.')
parser.add_argument('--disable-source-ssl', required=False, action='store_true', help='If set, disable source SSL.')
parser.add_argument('--disable-dest-ssl', required=False, action='store_true', help='If set, disable destination SSL.')
parser.add_argument('--cert-file', required=False, default=None, help='Cert file to use with SSL.')
parser.add_argument('--key-file', required=False, default=None, help='Key file to use with SSL.')
parser.add_argument('--ca-file', required=False, default=None, help='Certificate authority file to use for SSL.')
parser.add_argument('--create-only', required=False, action='store_true', help='If set, only create the index (with settings/mappings/aliases).')
parser.add_argument('-i', '--indices', required=False, default="*", help='Regular expression for indexes to copy.')
parser.add_argument('--name-override', required=False, default=None, help='destination index name override')

args = parser.parse_args()


def create_ssl_context():
    if args.cert_file is None:
        raise Error('--cert-file is required with SSL.')
    if args.key_file is None:
        raise Error('--key-file is required with SSL.')
    if args.ca_file is None:
        raise Error('--ca-file is required with SSL.')

    context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH,
        cafile=args.ca_file
    )
    context.load_cert_chain(
        certfile=args.cert_file,
        keyfile=args.key_file
    )

    return context


def create_client(host, ssl_context):
    return elasticsearch.Elasticsearch(
        [host],
        ssl_context=ssl_context
    )


class EsClients:
    def __init__(self, source_client, dest_client):
        self.source_client = source_client
        self.dest_client = dest_client


def get_index_settings(client, pattern):
    indices = elasticsearch.client.IndicesClient(client).get(pattern)
    return indices


def clean_settings(config):
    # Settings set by the server that we can read, but not write.
    del config['settings']['index']['provided_name']
    del config['settings']['index']['version']
    del config['settings']['index']['creation_date']
    del config['settings']['index']['uuid']
    return config


def find_max_ngram_diff_helper(obj):
    # Finds the greatest diff in ngram settings and returns the value. In Elasticsearch 7, an upper bound must be
    # explicitly set.
    if not isinstance(obj, dict):
        return -1

    diff = -1

    if 'min_gram' in obj and 'max_gram' in obj:
        diff = int(obj['max_gram']) - int(obj['min_gram'])

    for value in obj.values():
        t = find_max_ngram_diff_helper(value)
        diff = max(t, diff)

    return diff


def find_max_ngram_diff(config):
    settings = config['settings']
    return find_max_ngram_diff_helper(settings)


def update_for_seven(config):
    # Updates settings and mappings for Elasticsearch 7.

    # Should only be one value in 5 - the doc type. Unwrap for 7; document types are deprecated.
    config['mappings'] = next(iter(config['mappings'].values()))

    # Need to set max_ngram_diff if any ngram diffs are more than 1.
    max_ngram = find_max_ngram_diff(config)
    if max_ngram > 1:
        config['settings']['index']['max_ngram_diff'] = max_ngram

    # _all is deprecated and also false by default; so not even explicitly needed...
    if '_all' in config['mappings']:
        enabled = config['mappings']['_all']['enabled']
        if enabled:
            raise Error('_all is enabled')
        del config['mappings']['_all']

    return config


def create_index(client, name, config, name_override=None):
    name_override = name if name_override is None else name_override
    # Creates the given index on the client.
    indices_client = elasticsearch.client.IndicesClient(client)
    if indices_client.exists(name_override):
        print('WARNING: Index %s already exists!' % name_override)
        return
    indices_client.create(name_override, body=config)


timing_samples = []

# Copy pasted from source code so that we can transform documents while copying
def reindex(
        client,
        source_index,
        target_index,
        query=None,
        target_client=None,
        chunk_size=500,
        scroll="5m",
        scan_kwargs={},
        bulk_kwargs={},
):
    # Like the elasticsearch.helpers.reindex function, but with some custom logic. Namely, allows for source/dest
    # indices to be on different clusters, prints status updates, and deletes the _type field.

    target_client = client if target_client is None else target_client
    docs = elasticsearch.helpers.scan(client, query=query, index=source_index, scroll=scroll, **scan_kwargs)

    start = time.time()
    count = 0
    count_at_last_update = 0
    last_print = start
    update_interval = 5

    def _change_doc_index(hits, index):
        for h in hits:
            h["_index"] = index
            if "fields" in h:
                h.update(h.pop("fields"))

            # TODO: Need to remove "_type" otherwise it complains about keyword becoming text? Is this legitimate?
            if "_type" in h:
                del h["_type"]

            nonlocal count
            nonlocal last_print
            nonlocal count_at_last_update
            count = count + 1

            # Use a window of samples to average over.
            if (time.time() - last_print) > update_interval:
                timing_samples.append((count - count_at_last_update) / (time.time() - last_print))
                if len(timing_samples) > 10:
                    timing_samples.pop(0)
                count_at_last_update = count
                last_print = time.time()
                print('Transferring %s docs/second. Total %s.' % (sum(timing_samples) / len(timing_samples), count))

            yield h

    kwargs = {"stats_only": True}
    kwargs.update(bulk_kwargs)
    return elasticsearch.helpers.bulk(
        target_client,
        _change_doc_index(docs, target_index),
        chunk_size=chunk_size,
        raise_on_error=False,
        **kwargs
    )


def copy_index_data(clients, index, name_override):
    # Copies all documents from the source to the dest index.
    name_override = index if name_override is None else name_override
    print('Copying index %s' % index)
    start = time.time()
    res = reindex(
        clients.source_client,
        index,
        name_override,
        target_client=clients.dest_client
    )
    end = time.time()
    print('Documents written %s. Errors %s.' % res)
    print('Took %s seconds.' % (end - start))


def main():
    ssl_context = create_ssl_context() if not args.disable_source_ssl or not args.disable_dest_ssl else None
    source_ssl_context = ssl_context if not args.disable_source_ssl else None
    dest_ssl_context = ssl_context if not args.disable_dest_ssl else None
    clients = EsClients(create_client(args.source, source_ssl_context), create_client(args.dest, dest_ssl_context))
    indices = get_index_settings(clients.source_client, args.indices)

    def by_index(item):
        return item[0]

    # Sort for repeatability, and to make it easy to restart part way if the script failed.
    indexSorted = list(indices.items())
    indexSorted.sort(key=by_index)

    for index, config in indexSorted:
        # Skip this "hidden" index that is listed for some reason.
        if index == '.kibana':
            continue

        config = clean_settings(config)
        config = update_for_seven(config)
        print('Creating index %s' % (index if args.name_override is None else args.name_override))
        create_index(clients.dest_client, index, config, args.name_override)

    if args.create_only:
        return

    for index, config in indexSorted:
        copy_index_data(clients, index, args.name_override)


main()
