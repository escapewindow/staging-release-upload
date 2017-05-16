#!/usr/bin/env python
# s3cmd -c ~/.s3cfg-mozilla cp \
#   "s3://net-mozaws-prod-delivery-firefox/pub/firefox/tinderbox-builds/mozilla-release-win32/1491998108/firefox-53.0.en-US.win32.installer-stub.exe" \
#   "s3://net-mozaws-prod-delivery-firefox/pub/firefox/candidates/53.0-candidates/build5/partner-repacks/funnelcake110/v1/win32/en-US/Firefox Setup Stub 53.0.exe"

import aiohttp
import asyncio
import logging
import pprint
import sys
from taskcluster.async import Queue

log = logging.getLogger(__name__)
task_graph_id = "d996zZnrTJKne2LHXUTP6w"
signing_task_names = ["signing-linux-devedition-nightly/opt"]
num_expected_signing_tasks = 1
# map to copy artifacts to bucket - public/build/target.complete.mar

# where are the mac / win mars?
# https://archive.mozilla.org/pub/devedition/tinderbox-builds/jamun-macosx64-devedition/1494964280/


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


async def download():
    pass
    # windows / mac download from archive; linux download from task


async def upload():
    pass
    # upload from tmpdir. alternately an s3 copy would be preferable


async def async_main():
    with aiohttp.ClientSession() as session:
        queue = Queue(session=session)
        graph = await queue.listTaskGroup(task_graph_id)
        filtered = get_filtered(graph)
        log.debug("filtered: {}".format(filtered))
        if len(filtered) != num_expected_signing_tasks:
            die("Expected {} signing tasks; only found {}".format(
                num_expected_signing_tasks, filtered))
        await download()
        # checksums
        await upload()


def main():
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler())
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(async_main())


__name__ == '__main__' and main()
