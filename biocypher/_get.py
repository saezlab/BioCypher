"""
BioCypher get module. Used to download and cache data from external sources.
"""

from __future__ import annotations

import ftplib
import json
import os
import shutil

from abc import ABC
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
from typing import Optional

import pooch
import requests

from ._logger import logger
from ._misc import is_nested, to_list

logger.debug(f"Loading module {__name__}.")


class Resource(ABC):
    def __init__(
        self,
        name: str,
        url_s: str | list[str],
        lifetime: int = 0,
    ):
        """
        A Resource is a file, a list of files, an API request, or a list of API
        requests, any of which can be downloaded from the given URL(s) and
        cached locally. This class implements checks of the minimum requirements
        for a resource, to be implemented by a biocypher adapter.

        Args:
            name (str): The name of the resource.

            url_s (str | list[str]): The URL or URLs of the resource.

            lifetime (int): The lifetime of the resource in days. If 0, the
                resource is considered to be permanent.
        """
        self.name = name
        self.url_s = url_s
        self.lifetime = lifetime


class FileDownload(Resource):
    def __init__(
        self,
        name: str,
        url_s: str | list[str],
        lifetime: int = 0,
        is_dir: bool = False,
    ):
        """
        Represents basic information for a File Download.

        Args:
            name(str): The name of the File Download.

            url_s(str|list[str]): The URL(s) of the File Download.

            lifetime(int): The lifetime of the File Download in days. If 0, the
                File Download is cached indefinitely.

            is_dir (bool): Whether the URL points to a directory or not.
        """

        super().__init__(name, url_s, lifetime)
        self.is_dir = is_dir


class APIRequest(Resource):
    def __init__(self, name: str, url_s: str | list[str], lifetime: int = 0):
        """
        Represents basic information for an API Request.

        Args:
            name(str): The name of the API Request.

            url_s(str|list): The URL of the API endpoint.

            lifetime(int): The lifetime of the API Request in days. If 0, the
                API Request is cached indefinitely.

        """
        super().__init__(name, url_s, lifetime)


class Downloader:
    def __init__(self, cache_dir: Optional[str] = None) -> None:
        """
        The Downloader is a class that manages resources that can be downloaded
        and cached locally. It manages the lifetime of downloaded resources by
        keeping a JSON record of the download date of each resource.

        Args:
            cache_dir (str): The directory where the resources are cached. If
                not given, a temporary directory is created.
        """
        self.cache_dir = cache_dir or TemporaryDirectory().name
        self.cache_file = os.path.join(self.cache_dir, "cache.json")
        self.cache_dict = self._load_cache_dict()

    def download(self, *resources: Resource):
        """
        Download one or multiple resources. Load from cache if the resource is
        already downloaded and the cache is not expired.

        Args:
            resources (Resource): The resource(s) to download or load from
                cache.

        Returns:
            list[str]: The path or paths to the resource(s) that were downloaded
                or loaded from cache.

        """
        paths = []
        for resource in resources:
            paths.append(self._download_or_cache(resource))

        # flatten list if it is nested
        if is_nested(paths):
            paths = [path for sublist in paths for path in sublist]

        return paths

    def _download_or_cache(self, resource: Resource, cache: bool = True):
        """
        Download a resource if it is not cached or exceeded its lifetime.

        Args:
            resource (Resource): The resource to download.
        Returns:
            list[str]: The path or paths to the downloaded resource(s).

        """
        expired = self._is_cache_expired(resource)

        if expired or not cache:
            self._delete_expired_cache(resource)
            if isinstance(resource, FileDownload):
                logger.info(f"Asking for download of resource {resource.name}.")
                paths = self._download_files(cache, resource)
            elif isinstance(resource, APIRequest):
                logger.info(f"Asking for download of api request {resource.name}.")
                paths = self._download_api_request(resource)
            else:
                raise TypeError(f"Unknown resource type: {type(resource)}")
        else:
            paths = self.get_cached_version(resource)
        self._update_cache_record(resource)
        return paths

    def _is_cache_expired(self, resource: Resource) -> bool:
        """
        Check if resource or API request cache is expired.

        Args:
            resource (Resource): The resource to download.

        Returns:
            bool: cache is expired or not.
        """
        cache_record = self._get_cache_record(resource)
        if cache_record:
            download_time = datetime.strptime(cache_record.get("date_downloaded"), "%Y-%m-%d %H:%M:%S.%f")
            lifetime = timedelta(days=resource.lifetime)
            expired = download_time + lifetime < datetime.now()
        else:
            expired = True
        return expired

    def _delete_expired_cache(self, resource: Resource):
        cache_resource_path = self.cache_dir + "/" + resource.name
        if os.path.exists(cache_resource_path) and os.path.isdir(cache_resource_path):
            shutil.rmtree(cache_resource_path)

    def _download_files(self, cache, file_download: FileDownload):
        """
        Download a resource given it is a file or a directory and return the
        path.

        Args:
            cache (bool): Whether to cache the resource or not.
            file_download (FileDownload): The resource to download.

        Returns:
            list[str]: The path or paths to the downloaded resource(s).
        """
        if file_download.is_dir:
            files = self._get_files(file_download)
            file_download.url_s = [file_download.url_s + "/" + file for file in files]
            file_download.is_dir = False
            paths = self._download_or_cache(file_download, cache)
        elif isinstance(file_download.url_s, list):
            paths = []
            for url in file_download.url_s:
                fname = url[url.rfind("/") + 1 :].split("?")[0]
                path = self._retrieve(
                    url=url,
                    fname=fname,
                    path=os.path.join(self.cache_dir, file_download.name),
                )
                paths.append(path)
        else:
            paths = []
            fname = file_download.url_s[file_download.url_s.rfind("/") + 1 :].split("?")[0]
            results = self._retrieve(
                url=file_download.url_s,
                fname=fname,
                path=os.path.join(self.cache_dir, file_download.name),
            )
            if isinstance(results, list):
                paths.extend(results)
            else:
                paths.append(results)

        # sometimes a compressed file contains multiple files
        # TODO ask for a list of files in the archive to be used from the
        # adapter
        return paths

    def _download_api_request(self, api_request: APIRequest):
        """
        Download an API request and return the path.

        Args:
            api_request(APIRequest): The API request result that is being cached.
        Returns:
            list[str]: The path to the cached API request.

        """
        urls = api_request.url_s if isinstance(api_request.url_s, list) else [api_request.url_s]
        paths = []
        for url in urls:
            fname = url[url.rfind("/") + 1 :].rsplit(".", 1)[0]
            logger.info(f"Asking for caching API of {api_request.name} {fname}.")
            response = requests.get(url=url)

            if response.status_code != 200:
                response.raise_for_status()
            response_data = response.json()
            api_path = os.path.join(self.cache_dir, api_request.name, f"{fname}.json")

            os.makedirs(os.path.dirname(api_path), exist_ok=True)
            with open(api_path, "w") as f:
                json.dump(response_data, f)
                logger.info(f"Caching API request to {api_path}.")
            paths.append(api_path)
        return paths

    def get_cached_version(self, resource: Resource) -> list[str]:
        """Get the cached version of a resource.

        Args:
            resource(Resource): The resource to get the cached version of.

        Returns:
            list[str]: The paths to the cached resource(s).
        """
        cached_location = os.path.join(self.cache_dir, resource.name)
        logger.info(f"Use cached version from {cached_location}.")
        paths = []
        for file in os.listdir(cached_location):
            paths.append(os.path.join(cached_location, file))
        return paths

    def _retrieve(
        self,
        url: str,
        fname: str,
        path: str,
        known_hash: str = None,
    ):
        """
        Retrieve a file from a URL using Pooch. Infer type of file from
        extension and use appropriate processor.

        Args:
            url (str): The URL to retrieve the file from.

            fname (str): The name of the file.

            path (str): The path to the file.
        """
        if fname.endswith(".zip"):
            return pooch.retrieve(
                url=url,
                known_hash=known_hash,
                fname=fname,
                path=path,
                processor=pooch.Unzip(),
                progressbar=True,
            )

        elif fname.endswith(".tar.gz"):
            return pooch.retrieve(
                url=url,
                known_hash=known_hash,
                fname=fname,
                path=path,
                processor=pooch.Untar(),
                progressbar=True,
            )

        elif fname.endswith(".gz"):
            return pooch.retrieve(
                url=url,
                known_hash=known_hash,
                fname=fname,
                path=path,
                processor=pooch.Decompress(),
                progressbar=True,
            )

        else:
            return pooch.retrieve(
                url=url,
                known_hash=known_hash,
                fname=fname,
                path=path,
                progressbar=True,
            )

    def _get_files(self, file_download: FileDownload):
        """
        Get the files contained in a directory file.

        Args:
            file_download (FileDownload): The directory file.

        Returns:
            list: The files contained in the directory.
        """
        if file_download.url_s.startswith("ftp://"):
            # remove protocol
            url = file_download.url_s[6:]
            # get base url
            url = url[: url.find("/")]
            # get directory (remove initial slash as well)
            dir = file_download.url_s[7 + len(url) :]
            # get files
            ftp = ftplib.FTP(url)
            ftp.login()
            ftp.cwd(dir)
            files = ftp.nlst()
            ftp.quit()
        else:
            raise NotImplementedError("Only FTP directories are supported at the moment.")

        return files

    def _load_cache_dict(self):
        """
        Load the cache dictionary from the cache file. Create an empty cache
        file if it does not exist.
        """
        if not os.path.exists(self.cache_dir):
            logger.info(f"Creating cache directory {self.cache_dir}.")
            os.makedirs(self.cache_dir)

        if not os.path.exists(self.cache_file):
            logger.info(f"Creating cache file {self.cache_file}.")
            with open(self.cache_file, "w") as f:
                json.dump({}, f)

        with open(self.cache_file, "r") as f:
            logger.info(f"Loading cache file {self.cache_file}.")
            return json.load(f)

    def _get_cache_record(self, resource: Resource):
        """
        Get the cache record of a resource.

        Args:
            resource (Resource): The resource to get the cache record of.

        Returns:
            The cache record of the resource.
        """
        return self.cache_dict.get(resource.name, {})

    def _update_cache_record(self, resource: Resource):
        """
        Update the cache record of a resource.

        Args:
            resource (Resource): The resource to update the cache record of.
        """
        cache_record = {}
        cache_record["url"] = to_list(resource.url_s)
        cache_record["date_downloaded"] = str(datetime.now())
        cache_record["lifetime"] = resource.lifetime
        self.cache_dict[resource.name] = cache_record
        with open(self.cache_file, "w") as f:
            json.dump(self.cache_dict, f, default=str)
