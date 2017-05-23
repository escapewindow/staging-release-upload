#!/usr/bin/env python

import aiohttp
import asyncio
import datetime
import json
import logging
import math
import os
import sys
import tempfile

from mozapkpublisher.get_apk import check_apk_against_checksum_file as check_mar_against_checksum_file
from mozapkpublisher.utils import file_sha512sum
from beetmoverscript.script import upload_to_s3, move_beets, generate_beetmover_manifest
from scriptworker.utils import download_file, raise_future_exceptions

log = logging.getLogger(__name__)
# XXX This number defines the size of a batch. You need to batch uploads because we're throttled down on the server side.
# Batch are also needed because on S3, you first make a request to create an artifact. Then you upload it in a second
# request. Due to the number of files to upload, some files time out before their upload start.
# Another solution is to increase the timeout time. But if you have a good network connection (tested with an upload
# speed of 25 MB/s), it's just faster to have more connections.
max_concurrent_aiohttp_streams = 10
BUILD_NUMBER = 2    # XXX Edit this value
VERSION = '54.0b1'  # XXX Edit this value

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.json')


LATEST_MAR_LOCATIONS = {
    'win32': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition.win32-opt/artifacts/public/build/firefox-54.0.en-US.win32.complete.mar',           # NOQA
    'win64': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition.win64-opt/artifacts/public/build/firefox-54.0.en-US.win64.complete.mar',           # NOQA
    'mac': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition.macosx64-opt/artifacts/public/build/firefox-54.0.en-US.mac.complete.mar',            # NOQA
    'linux-i686': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition-l10n.linux-opt.en-US/artifacts/public/build/update/target.complete.mar',      # NOQA
    'linux-x86_64': 'https://index.taskcluster.net/v1/task/gecko.v2.jamun.latest.devedition-l10n.linux64-opt.en-US/artifacts/public/build/update/target.complete.mar',  # NOQA
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
        log.info('Uploading files for platform "{}"...'.format(platform))
        await _upload_platform_batch_by_batch(context, platform, details)

    log.info('Uploading SHA512SUMS...')
    await _upload_general_sha512sums_file(context, path_with_sha512sums_per_platform)
    log.info('Uploaded SHA512SUMS')


async def _upload_platform_batch_by_batch(context, platform, platform_details):
    # We wait for a batch to finish. This allows to have a decent number of parallel uploads,
    # And reduces the number of timeouts. See max_concurrent_aiohttp_streams for more details
    batch_size = max_concurrent_aiohttp_streams
    total_number_of_batches = math.ceil(len(ALL_LOCALES) / batch_size)
    for i in range(total_number_of_batches):
        batch_lower_bound = i * batch_size
        batch_upper_bound = (i + 1) * batch_size
        batch_of_locales = ALL_LOCALES[batch_lower_bound:batch_upper_bound]

        log.info('{platform}: Uploading batch {batch_number}/{total_batches} (locales: {locales})...'.format(
            platform=platform, batch_number=i+1, total_batches=total_number_of_batches, locales=batch_of_locales
        ))

        await _upload_one_batch(context, platform_details, batch_of_locales)
        log.info('{}: Batch {}/{} uploaded'.format(platform, i+1, total_number_of_batches+1))


async def _upload_one_batch(context, platform_details, batch_of_locales):
    batched_tasks = []
    for locale in batch_of_locales:
        context.task['payload']['locale'] = locale
        mapping_manifest = generate_beetmover_manifest(context)

        artifact_name = 'target.complete.mar'
        artifacts_to_beetmove = {
            locale: {
                artifact_name: platform_details['mar'],
            },
        }

        batched_tasks.append(
            asyncio.ensure_future(
                move_beets(context, artifacts_to_beetmove, mapping_manifest)
            )
        )

    await raise_future_exceptions(batched_tasks)


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
