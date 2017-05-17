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
        platform_sub_folder = get_platform_base_folder(context, platform)

        mar_abs_path = os.path.join(platform_sub_folder, get_sub_path_from_url(mar_url))
        await download_file(context, url=mar_url, abs_filename=mar_abs_path)

        checksums_url = mar_url.replace('.complete.mar', '.checksums')
        checksums_url = checksums_url.replace('update/', '')
        checksums_abs_path = os.path.join(platform_sub_folder, get_sub_path_from_url(checksums_url))
        await download_file(context, url=checksums_url, abs_filename=checksums_abs_path)

        abs_path_per_platform[platform] = {
            'mar': mar_abs_path,
            'checksums': checksums_abs_path,
        }

    return abs_path_per_platform


def get_platform_base_folder(context, platform):
    return os.path.join(context['tmp_dir'], platform)


def get_sub_path_from_url(url):
    return 'update/{}'.format(url.split('/')[-1])


async def download_file(context, url, abs_filename, session=None, chunk_size=128):
    """ From https://github.com/mozilla-releng/scriptworker/blob/65b110f21fed199268062191ec9cb34a2bf878ea/scriptworker/utils.py#L420
    """
    log.info("Downloading %s", url)
    session = context['session']
    parent_dir = os.path.dirname(abs_filename)
    async with session.get(url) as resp:
        if resp.status != 200:
            raise IOError("{} status {} is not 200!".format(url, resp.status))
        makedirs(parent_dir)
        with open(abs_filename, 'wb') as fd:
            while True:
                chunk = await resp.content.read(chunk_size)
                if not chunk:
                    break
                fd.write(chunk)
    log.info("Done")


def makedirs(path):
    """ From https://github.com/mozilla-releng/scriptworker/blob/65b110f21fed199268062191ec9cb34a2bf878ea/scriptworker/utils.py#L131
    """
    if path:
        if not os.path.exists(path):
            log.debug("makedirs({})".format(path))
            os.makedirs(path)
        else:
            realpath = os.path.realpath(path)
            if not os.path.isdir(realpath):
                raise ScriptWorkerException(
                    "makedirs: {} already exists and is not a directory!".format(path)
                )


def checksums(context, abs_path_per_platform):
    for platform, abs_paths in abs_path_per_platform.items():
        check_mar_against_checksum_file(context, abs_paths['mar'], abs_paths['checksums'])


def check_mar_against_checksum_file(context, mar_file, checksum_file):
    """ From https://github.com/mozilla-releng/mozapkpublisher/blob/3a2fcf264e7ac900ad9476ae5beeb2878585bcaa/mozapkpublisher/get_apk.py#L148
    """
    log.debug('Checking checksum for "{}"...'.format(mar_file))

    checksum = _fetch_checksum_from_file(context, checksum_file, mar_file)
    mar_checksum = file_sha512sum(mar_file)

    if checksum == mar_checksum:
        log.info('Checksum for "{}" succeeded!'.format(mar_file))
        os.remove(checksum_file)
    else:
        raise CheckSumMismatch(mar_file, expected=mar_checksum, actual=checksum)


def _fetch_checksum_from_file(context, checksum_file, mar_file):
    """ From https://github.com/mozilla-releng/mozapkpublisher/blob/3a2fcf264e7ac900ad9476ae5beeb2878585bcaa/mozapkpublisher/get_apk.py#L161
    """
    base_mar_path = remove_base_folder(context, mar_file)
    with open(checksum_file, 'r') as fh:
        for line in fh:
            m = re.match(r"""^(?P<hash>.*) sha512 (?P<filesize>\d+) {}""".format(base_mar_path), line)
            if m:
                gd = m.groupdict()
                log.info("Found hash {}".format(gd['hash']))
                return gd['hash']

    raise Exception('Hash not found in "{}"'.format(checksum_file))


def remove_base_folder(context, file_path):
    platform = [
        platform for platform in LATEST_MAR_LOCATIONS if '{}/'.format(platform) in file_path
    ][0]
    part_to_delete = '{}/'.format(get_platform_base_folder(context, platform))
    return file_path.replace(part_to_delete, '')


def file_sha512sum(file_path):
    """ From https://github.com/mozilla-releng/mozapkpublisher/blob/3a2fcf264e7ac900ad9476ae5beeb2878585bcaa/mozapkpublisher/utils.py#L19
    """
    bs = 65536
    hasher = hashlib.sha512()
    with open(file_path, 'rb') as fh:
        buf = fh.read(bs)
        while len(buf) > 0:
            hasher.update(buf)
            buf = fh.read(bs)
    return hasher.hexdigest()


class CheckSumMismatch(Exception):
    """ From https://github.com/mozilla-releng/mozapkpublisher/blob/3a2fcf264e7ac900ad9476ae5beeb2878585bcaa/mozapkpublisher/exceptions.py#L16
    """
    def __init__(self, checked_file, expected, actual):
        super(CheckSumMismatch, self).__init__(
            'Downloading "{}" failed!. Checksum "{}" was expected, but actually got "{}"'
            .format(checked_file, expected, actual)
        )


async def upload():
    pass
    # upload from tmpdir. alternately an s3 copy would be preferable


async def async_main():
    conn = aiohttp.TCPConnector(limit=max_concurrent_aiohttp_streams)
    with aiohttp.ClientSession(connector=conn) as session:

        with tempfile.TemporaryDirectory() as tmp_dir:
            context = {
                'session': session,
                'tmp_dir': tmp_dir,
            }
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
