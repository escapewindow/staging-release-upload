#!/usr/bin/env python
# s3cmd -c ~/.s3cfg-mozilla cp \
#   "s3://net-mozaws-prod-delivery-firefox/pub/firefox/tinderbox-builds/mozilla-release-win32/1491998108/firefox-53.0.en-US.win32.installer-stub.exe" \
#   "s3://net-mozaws-prod-delivery-firefox/pub/firefox/candidates/53.0-candidates/build5/partner-repacks/funnelcake110/v1/win32/en-US/Firefox Setup Stub 53.0.exe"

import aiohttp
import asyncio
import logging
import pprint
from  taskcluster.async import Queue

task_graph_id = "d996zZnrTJKne2LHXUTP6w"
signing_task_names = ["signing-linux-devedition-nightly/opt"]
num_expected_signing_tasks = 1

# https://archive.mozilla.org/pub/devedition/tinderbox-builds/jamun-macosx64-devedition/1494964280/


def get_filtered(graph):
    filtered = [
        e['status']['taskId'] for e in graph['tasks']
            if e['status']['state'] in ['completed']
            and e['status']['workerType'] in ['signing-linux-v1']
            and e['task']['metadata']['name'] in signing_task_names
    ]
    return filtered


async def async_main():
    with aiohttp.ClientSession() as session:
        queue = Queue(session=session)
        graph = await queue.listTaskGroup(task_graph_id)
        pprint.pprint(get_filtered(graph))
        # TODO verify num signing tasks
        # map to copy artifacts to bucket - public/build/target.complete.mar
        # windows
        # mac


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(async_main())
