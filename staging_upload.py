#!/usr/bin/env python
# s3cmd -c ~/.s3cfg-mozilla cp \
#   "s3://net-mozaws-prod-delivery-firefox/pub/firefox/tinderbox-builds/mozilla-release-win32/1491998108/firefox-53.0.en-US.win32.installer-stub.exe" \
#   "s3://net-mozaws-prod-delivery-firefox/pub/firefox/candidates/53.0-candidates/build5/partner-repacks/funnelcake110/v1/win32/en-US/Firefox Setup Stub 53.0.exe"

import aiohttp
import asyncio
import logging
import hashlib
import pprint
import os
import re
import sys
import tempfile

from mozapkpublisher.get_apk import check_apk_against_checksum_file as check_mar_against_checksum_file
from scriptworker.utils import download_file
from taskcluster.async import Queue

log = logging.getLogger(__name__)
task_graph_id = "d996zZnrTJKne2LHXUTP6w"
signing_task_names = ["signing-linux-devedition-nightly/opt"]
num_expected_signing_tasks = 1
max_concurrent_aiohttp_streams = 10
# map to copy artifacts to bucket - public/build/target.complete.mar


LATEST_MAR_LOCATIONS = {
    'win32': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition.win32-opt/artifacts/public/build/firefox-54.0.en-US.win32.complete.mar',
    'win64': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition.win64-opt/artifacts/public/build/firefox-54.0.en-US.win64.complete.mar',
    'macosx64': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition.macosx64-opt/artifacts/public/build/firefox-54.0.en-US.mac.complete.mar',
    'linux': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition-l10n.linux-opt.en-US/artifacts/public/build/update/target.complete.mar',
    'linux64': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition-l10n.linux64-opt.en-US/artifacts/public/build/update/target.complete.mar',
}

def get_filtered(graph):
    filtered = [
        e['status']['taskId'] for e in graph['tasks']
        if e['status']['state'] in ['completed']
        and e['status']['workerType'] in ['signing-linux-v1']
        and e['task']['metadata']['name'] in signing_task_names
    ]
    return filtered


def die(msg):
    log.critical(msg)
    sys.exit(1)


async def download(context):
    abs_path_per_platform = {}
    for platform, mar_url in LATEST_MAR_LOCATIONS.items():
        log.info('Downloading {}'.format(platform))
        platform_sub_folder = os.path.join(context.tmp_dir, platform)

        mar_abs_path = os.path.join(platform_sub_folder, 'update', get_filename_from_url(mar_url))
        await download_file(context, url=mar_url, abs_filename=mar_abs_path)

        checksums_url = mar_url.replace('.complete.mar', '.checksums')
        checksums_url = checksums_url.replace('update/', '')
        checksums_abs_path = os.path.join(platform_sub_folder, get_filename_from_url(checksums_url))
        await download_file(context, url=checksums_url, abs_filename=checksums_abs_path)

        abs_path_per_platform[platform] = {
            'mar': mar_abs_path,
            'checksums': checksums_abs_path,
        }

    return abs_path_per_platform


def get_filename_from_url(url):
    return url.split('/')[-1]


def checksums(context, abs_path_per_platform):
    for platform, abs_paths in abs_path_per_platform.items():
        check_mar_against_checksum_file(abs_paths['mar'], abs_paths['checksums'])


async def upload():
    pass
    # upload from tmpdir. alternately an s3 copy would be preferable


class Context:
    def __init__(self, session, tmp_dir):
        self.session = session
        self.tmp_dir = tmp_dir


async def async_main():
    conn = aiohttp.TCPConnector(limit=max_concurrent_aiohttp_streams)
    with aiohttp.ClientSession(connector=conn) as session:

        with tempfile.TemporaryDirectory() as tmp_dir:
            context = Context(session, tmp_dir)
            queue = Queue(session=session)
            graph = await queue.listTaskGroup(task_graph_id)
            filtered = get_filtered(graph)
            log.debug("filtered: {}".format(filtered))
            if len(filtered) != num_expected_signing_tasks:
                die("Expected {} signing tasks; only found {}".format(
                    num_expected_signing_tasks, filtered))

            abs_path_per_platform = await download(context)
            checksums(context, abs_path_per_platform)
            await upload()


def main():
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler())
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(async_main())


__name__ == '__main__' and main()
