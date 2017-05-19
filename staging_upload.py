#!/usr/bin/env python
# s3cmd -c ~/.s3cfg-mozilla cp \
#   "s3://net-mozaws-prod-delivery-firefox/pub/firefox/tinderbox-builds/mozilla-release-win32/1491998108/firefox-53.0.en-US.win32.installer-stub.exe" \
#   "s3://net-mozaws-prod-delivery-firefox/pub/firefox/candidates/53.0-candidates/build5/partner-repacks/funnelcake110/v1/win32/en-US/Firefox Setup Stub 53.0.exe"

import aiohttp
import asyncio
import datetime
import hashlib
import json
import logging
import pprint
import os
import re
import sys
import tempfile

from mozapkpublisher.get_apk import check_apk_against_checksum_file as check_mar_against_checksum_file
from mozapkpublisher.utils import file_sha512sum
from beetmoverscript.script import upload_to_s3, move_beets, generate_beetmover_manifest
from scriptworker.utils import download_file, raise_future_exceptions

log = logging.getLogger(__name__)
# XXX There are less than 100 locales to upload. Having fewer connection open leads to about 5 timeouts. Here's why:
# On S3, you first make a request to create an artifact. Then you upload it in a second request. Due to the number of
# files to upload, some files time out before their upload start.
# Another solution is to increase the timeout time. But if you have a good network connection (tested with an upload
# speed of 25 MB/s), it's just faster to have more connections.
max_concurrent_aiohttp_streams = 100
BUILD_NUMBER = 1    # XXX Edit this value
VERSION = '54.0b1'  # XXX Edit this value

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.json')


LATEST_MAR_LOCATIONS = {
    'win32': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition.win32-opt/artifacts/public/build/firefox-54.0.en-US.win32.complete.mar',
    'win64': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition.win64-opt/artifacts/public/build/firefox-54.0.en-US.win64.complete.mar',
    'mac': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition.macosx64-opt/artifacts/public/build/firefox-54.0.en-US.mac.complete.mar',
    'linux-i686': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition-l10n.linux-opt.en-US/artifacts/public/build/update/target.complete.mar',
    'linux-x86_64': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition-l10n.linux64-opt.en-US/artifacts/public/build/update/target.complete.mar',
}

ALL_LOCALES = (
  'ach', 'af', 'an', 'ar', 'as', 'ast', 'az', 'bg', 'bn-BD', 'bn-IN', 'br', 'bs',
  'ca', 'cak', 'cs', 'cy', 'da', 'de', 'dsb', 'el', 'en-GB', 'en-US', 'en-ZA',
  'eo', 'es-AR', 'es-CL', 'es-ES', 'es-MX', 'et', 'eu', 'fa', 'ff', 'fi', 'fr',
  'fy-NL', 'ga-IE', 'gd', 'gl', 'gn', 'gu-IN', 'he', 'hi-IN', 'hr', 'hsb', 'hu',
  'hy-AM', 'id', 'is', 'it', 'ja-JP-mac', 'ka', 'kab', 'kk', 'km', 'kn', 'ko',
  'lij', 'lt', 'lv', 'mai', 'mk', 'ml', 'mr', 'ms', 'nb-NO', 'nl', 'nn-NO', 'or',
  'pa-IN', 'pl', 'pt-BR', 'pt-PT', 'rm', 'ro', 'ru', 'si', 'sk', 'sl', 'son',
  'sq', 'sr', 'sv-SE', 'ta', 'te', 'th', 'tr', 'uk', 'ur', 'uz', 'vi', 'xh',
  'zh-CN', 'zh-TW',
)

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
    tasks = []

    for platform, mar_url in LATEST_MAR_LOCATIONS.items():
        log.info('Downloading {}...'.format(platform))
        platform_sub_folder = os.path.join(context.tmp_dir, platform)

        mar_abs_path = os.path.join(platform_sub_folder, 'update', get_filename_from_url(mar_url))
        tasks.append(
            asyncio.ensure_future(
                download_file(context, url=mar_url, abs_filename=mar_abs_path)
            )
        )

        checksums_url = mar_url.replace('.complete.mar', '.checksums')
        checksums_url = checksums_url.replace('update/', '')
        checksums_abs_path = os.path.join(platform_sub_folder, get_filename_from_url(checksums_url))
        tasks.append(
            asyncio.ensure_future(
                download_file(context, url=checksums_url, abs_filename=checksums_abs_path)
            )
        )

        abs_path_per_platform[platform] = {
            'mar': mar_abs_path,
            'checksums': checksums_abs_path,
        }

    await raise_future_exceptions(tasks)

    return abs_path_per_platform


def get_filename_from_url(url):
    return url.split('/')[-1]


def checksums(context, abs_path_per_platform):
    path_with_sha512sums_per_platform = {}

    for platform, abs_paths in abs_path_per_platform.items():
        mar_path = abs_paths['mar']
        check_mar_against_checksum_file(mar_path, abs_paths['checksums'])

        path_with_sha512sums_per_platform[platform] = {
            'mar': mar_path,
            'sha512': file_sha512sum(mar_path)
        }

    log.info('Checksum verification passed')
    return path_with_sha512sums_per_platform


async def upload(context, path_with_sha512sums_per_platform):


    for platform, details in path_with_sha512sums_per_platform.items():
        context.release_props['platform'] = platform

        tasks = []
        log.info('Uploading files for platform "{}"...'.format(platform))
        for locale in ALL_LOCALES:
            context.task['payload']['locale'] = locale
            mapping_manifest = generate_beetmover_manifest(context)

            artifact_name = 'target.complete.mar'
            artifacts_to_beetmove = {
                locale: {
                    artifact_name: details['mar'],
                },
            }

            tasks.append(
                asyncio.ensure_future(
                    move_beets(context, artifacts_to_beetmove, mapping_manifest)
                )
            )


        # We wait for each platform to finish. This allows to have a decent number of parallel uploads,
        # And reduces the number of timeouts. See max_concurrent_aiohttp_streams for more details
        await raise_future_exceptions(tasks)
        log.info('Uploaded all files for platform "{}"'.format(platform))

    log.info('Uploading SHA512SUMS...')
    await _upload_general_sha512sums_file(context, path_with_sha512sums_per_platform)
    log.info('Uploaded SHA512SUMS')

async def _upload_general_sha512sums_file(context, path_with_sha512sums_per_platform):
    general_shasum_file = _generate_sha512sums_file(context, path_with_sha512sums_per_platform)
    await upload_to_s3(
        context=context,
        # TODO this logic should not duplicate the one in move_beets()
        s3_key='pub/devedition/candidates/{}-candidates/build{}/SHA512SUMS'.format(VERSION, BUILD_NUMBER),
        path=general_shasum_file
    )


def _generate_sha512sums_file(context, path_with_sha512sums_per_platform):
    # TODO this logic should not duplicate the one in move_beets()
    lines = [
        '{sha512}  update/{platform}/{locale}/devedition-{version}.complete.mar\n'.format(
            sha512=details['sha512'], platform=platform, locale=locale, version=VERSION,
        )
        for platform, details in path_with_sha512sums_per_platform.items()
        for locale in ALL_LOCALES
    ]

    # .txt is to make python guess 'text/plain'. Otherwise None is returned
    general_shasum_file = os.path.join(context.tmp_dir, 'SHA512SUMS.txt')

    with open(general_shasum_file, 'w') as f:
        f.writelines(lines)

    return general_shasum_file


class Context:
    def __init__(self, session, tmp_dir):
        self.session = session
        self.tmp_dir = tmp_dir
        self.release_props = {
            'appName': 'devedition',
            'appVersion': VERSION,
            'branch': 'mozilla-beta',
            'stage_platform': 'firefox',
            'platform': 'TO_BE_CHANGED',
        }
        self.task = {
            'payload': {
                'locale': 'TO_BE_CHANGED',
                'upload_date': datetime.datetime.utcnow(),
                'build_number': BUILD_NUMBER,
                'version': VERSION,
            }
        }
        self.action = 'push-to-candidates'
        self.bucket = 'release'
        with open(CONFIG_FILE) as f:
            self.config = json.load(f)

        self.checksums = {}

async def async_main():
    conn = aiohttp.TCPConnector(limit=max_concurrent_aiohttp_streams)
    with aiohttp.ClientSession(connector=conn) as session:

        with tempfile.TemporaryDirectory() as tmp_dir:
            context = Context(session, tmp_dir)
            abs_path_per_platform = await download(context)
            log.info('All files downloaded')
            path_with_sha512sums_per_platform = checksums(context, abs_path_per_platform)

            await upload(context, path_with_sha512sums_per_platform)


def main():
    FORMAT = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(async_main())


__name__ == '__main__' and main()
