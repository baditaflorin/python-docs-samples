#!/usr/bin/env python

# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Command-line app to perform async queries with parameters in BigQuery.

For more information, see the README.md under /bigquery.

Example invocation:
    $ python async_query_params.py --use-named-params 'romeoandjuliet' 100
    $ python async_query_params.py --use-positional-params 'romeoandjuliet' 100
"""

import argparse
import time
import uuid

from google.cloud import bigquery


def wait_for_job(job):
    while True:
        job.reload()  # Refreshes the state via a GET request.
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


def print_results(query_results):
    """Print the query results by requesting a page at a time."""
    page_token = None

    while True:
        rows, total_rows, page_token = query_results.fetch_data(
            max_results=10,
            page_token=page_token)

        for row in rows:
            print(row)

        if not page_token:
            break


def async_query_positional_params(corpus, min_word_count):
    client = bigquery.Client()
    query_job = client.run_async_query(
        str(uuid.uuid4()),
        """SELECT word, word_count
        FROM `bigquery-public-data.samples.shakespeare`
        WHERE corpus = ?
        AND word_count >= ?
        ORDER BY word_count DESC;
        """,
        query_parameters=(
            bigquery.ScalarQueryParameter(
                # Set the name to None to use positional parameters (? symbol
                # in the query).  Note that you cannot mix named and positional
                # parameters.
                None,
                'STRING',
                corpus),
            bigquery.ScalarQueryParameter(None, 'INT64', min_word_count)))

    # Only standard SQL syntax supports parameters in queries.
    # See: https://cloud.google.com/bigquery/sql-reference/
    query_job.use_legacy_sql = False
    query_job.begin()
    wait_for_job(query_job)
    print_results(query_job.results())


def async_query_named_params(corpus, min_word_count):
    client = bigquery.Client()
    query_job = client.run_async_query(
        str(uuid.uuid4()),
        """SELECT word, word_count
        FROM `bigquery-public-data.samples.shakespeare`
        WHERE corpus = @corpus
        AND word_count >= @min_word_count
        ORDER BY word_count DESC;
        """,
        query_parameters=(
            bigquery.ScalarQueryParameter('corpus', 'STRING', corpus),
            bigquery.ScalarQueryParameter(
                'min_word_count',
                'INT64',
                min_word_count)))
    query_job.use_legacy_sql = False
    query_job.begin()
    wait_for_job(query_job)
    print_results(query_job.results())


def main(use_named_params=False, corpus='romeoandjuliet', min_word_count=100):
    if use_named_params:
        async_query_named_params(corpus, min_word_count)
    else:
        async_query_positional_params(corpus, min_word_count)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'corpus',
        help='Corpus to search from Shakespeare dataset.')
    parser.add_argument(
        'min_word_count',
        help='Minimum count of words to query.',
        type=int)

    params_type_parser = parser.add_mutually_exclusive_group(required=False)
    params_type_parser.add_argument(
        '--use-named-params',
        dest='use_named_params',
        action='store_true')
    params_type_parser.add_argument(
        '--use-positional-params',
        dest='use_named_params',
        action='store_false')
    parser.set_defaults(use_named_params=False)
    args = parser.parse_args()
    main(args.use_named_params, args.corpus, args.min_word_count)
