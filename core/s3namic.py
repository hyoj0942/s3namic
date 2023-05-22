from boto3 import client
from botocore import config
import csv
import pandas as pd
import json
import pickle
import gzip
import bz2
import io
from PIL import Image

class s3namic:
    """
    module for accessing s3 bucket and performing various tasks

    Example:
        >>> s3 = s3namic(bucket, access_key, secret_key, region)
    """
    def __init__(
        self,
        bucket: str,
        access_key: str,
        secret_key: str, 
        region: str,
    ): 
        self.__bucket = bucket
        self.__s3 = client(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=config.Config(
                read_timeout=60 * 60,  # an hour
                connect_timeout=60 * 60,  # an hour
                retries={"max_attempts": 0},
            ),
        )
        self.paginator = self.__s3.get_paginator("list_objects_v2")

    def _write_file(
        self,
        file_name: str,
        file_content: str,
        compression: str = None,
        encoding: str = "utf-8",
    ):
        """
        Write files to s3 bucket
        Write files to s3 bucket
        s3.upload_file() reads the file and uploads it to memory, and s3.put_object() writes the file directly without reading it, so memory usage is small.
        Use s3.upload_file() to upload a local file to your s3 bucket, and use s3.put_object (corresponding method) to directly write the contents of a file in your code.

        Args:
            file_name (str): file name (including path)
            file_content (str): file content
            compression (str): compression method (gzip, bz2) (default: None)
            encoding (str): encoding method (default: utf-8)

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3._write_file(file_name='test.txt', file_content='test')
        """
        file_content = (
            file_content.encode(encoding)
            if isinstance(file_content, str)
            else file_content
        )
        if compression == "gzip":
            file_content = gzip.compress(file_content)
            file_name = (
                f"{file_name}.gz" if not file_name.endswith(".gz") else file_name
            )
        elif compression == "bz2":
            file_content = bz2.compress(file_content)
            file_name = (
                f"{file_name}.bz2" if not file_name.endswith(".bz2") else file_name
            )

        self.__s3.put_object(
            Bucket=self.__bucket,
            Key=file_name,
            Body=file_content,
        )

    def _read_file(self, file_name: str):
        """
        Read files from s3 bucket

        Args:
            file_name (str): file name (including path)
        """
        compress = (
            "gzip"
            if file_name.endswith(".gz")
            else "bz2"
            if file_name.endswith(".bz2")
            else None
        )

        self.__obj = self.__s3.get_object(
            Bucket=self.__bucket,
            Key=file_name,
        )
        # read file contents depending on whether the file is compressed
        self.read_content = (
            gzip.decompress(self.__obj["Body"].read())
            if compress == "gzip"
            else bz2.decompress(self.__obj["Body"].read())
            if compress == "bz2"
            else self.__obj["Body"].read()
        )

    def read_image(self, file_name: str, to_image: bool = False):
        """
        Read image from s3 bucket

        Args:
            file_name (str): file name (including path)
            to_image (bool): Whether to convert to image (default: False)

        Returns:
            bytes: image content
            Image: image

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.read_image('test.png')
        """
        self._read_file(file_name)
        file_stream = io.BytesIO(self.read_content)
        if not to_image:
            return file_stream
        else:
            return Image.open(file_stream)

    def _thread_map(self, func, iterable: list, max_workers: int = 4) -> list:
        """
        쓰레드를 사용하여 함수를 병렬로 실행하는 함수

        Args:
            func (function): function
            iterable (list): Iterable object
            workers (int): number of threads (default: 4)
                - If the workers parameter is not used, set the number of threads to 4
                - It is recommended to set the thread count automatically using os.cpu_count()

        Returns:
            list: result

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3._thread_map(lambda x: x, [1, 2, 3])
        """
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(func, iterable))

    def upload_file(self, file_path: str, file_name: str):
        """
        Upload files to s3 bucket

        Args:
            file_path (str): file path
            file_name (str): file name

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.upload_file('/home/test.txt', 'test.txt')
        """
        self.__s3.upload_file(file_path, self.__bucket, file_name)

    def delete_file(self, file_name: str):
        """
        Delete files from s3 bucket

        Args:
            file_name (str): file name (including path)

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.delete_file('test.txt')
        """
        self.__s3.delete_object(Bucket=self.__bucket, Key=file_name)

    def download_file(self, file_name: str, load_path: str):
        """
        Download file from s3 bucket

        Args:
            file_name (str): file name (including path)
            load_path (str): download path

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.download_file('test.txt', '/home/test.txt')
        """
        self.__s3.download_file(self.__bucket, file_name, load_path)

    def compress(self, file_name: str, compression: str = "gzip"):
        """
        Compress files in s3 bucket and save them as files (using _read_file() method, _write_file() method)

        Args:
            file_name (str): file name (including path)
            compression (str): compression method (gzip, bz2) (default: gzip)

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3._comp_gzip(file_name='test.txt')
        """
        self._read_file(file_name)
        self._write_file(
            file_name=f"{file_name}.gz", file_content=gzip.compress(self.read_content)
        ) if compression == "gzip" else self._write_file(
            file_name=f"{file_name}.bz2", file_content=bz2.compress(self.read_content)
        ) if compression == "bz2" else print(
            "Please enter the exact compression method."
        )

        print(
            f'The file {file_name} was compressed using {compression} and saved as {file_name}.{"gz" if compression == "gzip" else "bz2"}.'
        )

    def decompress(self, file_name: str):
        """
        Unzip the file in the s3 bucket and save it as a file (using the _read_file() method, _write_file() method)

        Args:
            file_name (str): file name (including path)

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.decompress(file_name='test.txt.gz')
        """
        self._read_file(file_name)
        self._write_file(
            file_name=file_name.replace(".gz", "").replace(".bz2", ""),
            file_content=self.read_content,
        )

        print(
            f'The file {file_name} was unzipped and saved as {file_name.replace(".gz", "").replace(".bz2", "")}.'
        )

    def get_file_url(self, file_name: str, expires_in: int = 3600) -> str:
        """
        Get file url from s3 bucket

        Args:
            file_name (str): file name (including path)
            expires_in (int): URL validity time in seconds (default: 3600)

        Returns:
            str: file URL

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.get_file_url('test.txt')
        """
        return self.__s3.generate_presigned_url(  # get_file_url
            "get_object",  # List["get_object", "put_object", "delete_object"]
            Params={"Bucket": self.__bucket, "Key": file_name},
            ExpiresIn=expires_in,
        )  # URL validity time in seconds

    def read_csv(
        self,
        file_name: str,
        encoding: str = "utf-8",
        sep: str = ",",
        data_frame: bool = True,
        **kwargs,
    ):
        """
        Read csv file from s3 bucket

        Args:
            file_name (str): file name (including path)
            encoding (str): encoding (default: utf-8)
            sep (str): separator (default: ,)
            data_frame (bool): return type (default: True)
            **kwargs: arguments for pd.DataFrame
                document: https://pd.pydata.org/pandas-docs/stable/reference/api/pd.read_csv.html

        Returns:
            pd.DataFrame: csv file

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.read_csv(file_name='test.csv', encoding='utf-8', **kwargs)
        """
        self._read_file(file_name=file_name)
        splits = list(
            csv.reader(self.read_content.decode(encoding).splitlines(), delimiter=sep)
        )

        return (
            splits
            if not data_frame
            else pd.DataFrame(splits[1:], columns=splits[0], **kwargs)
        )

    def read_excel(
        self, file_name: str, encoding: str = "utf-8", **kwargs
    ) -> pd.DataFrame:
        """
        Read excel file from s3 bucket

        Args:
            file_name (str): file name (including path)
            encoding (str): encoding (default: utf-8)
            **kwargs: pd.read_excel()의 인자
                document: https://pd.pydata.org/pandas-docs/stable/reference/api/pd.read_excel.html

        Returns:
            pd.DataFrame: excel file

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.read_excel(file_name='test.xlsx', sheet_name=0, header=0, names=None, **kwargs)
        """
        self._read_file(file_name=file_name)

        return pd.read_excel(io.BytesIO(self.read_content), **kwargs)

    def read_json(self, file_name: str, encoding: str = "utf-8", **kwargs) -> dict:
        """
        Read json file from s3 bucket

        Args:
            file_name (str): file name (including path)
            encoding (str): encoding (default: utf-8)
            **kwargs: arguments for json.loads
                document: https://docs.python.org/ko/3/library/json.html

        Returns:
            dict: json file

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3._read_json(file_name='test.json', encoding='utf-8')
        """
        self._read_file(file_name)
        return json.loads(self.read_content.decode(encoding), **kwargs)

    def read_txt(self, file_name: str, encoding: str = "utf-8") -> str:
        """
        Read txt file from s3 bucket

        Args:
            file_name (str): file name (including path)
            encoding (str): encoding (default: utf-8)

        Returns:
            str: txt file

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3._read_txt(file_name='test.txt', encoding='utf-8')
        """
        self._read_file(file_name)
        return self.read_content.decode(encoding)

    def read_pkl(
        self, file_name: str, encoding: str = "utf-8", **kwargs
    ) -> pd.DataFrame:
        """
        Read pickle file from s3 bucket

        Args:
            file_name (str): file name (including path)
            encoding (str): encoding (default: utf-8)
            **kwargs: arguments for pickle.load
                document: https://docs.python.org/ko/3/library/pickle.html

        Returns:
            pd.DataFrame: pickle file

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3._read_pkl(file_name='test.pickle', encoding='utf-8')
        """
        self._read_file(file_name)
        return pickle.loads(self.read_content.decode(encoding), **kwargs)

    def read_parquet(self, file_name: str, **kwargs) -> pd.DataFrame:
        """
        Read parquet file from s3 bucket

        Args:
            file_name (str): file name (including path)
            encoding (str): encoding (default: utf-8)
            **kwargs: arguments for pd.read_parquet
                document: https://pd.pydata.org/pandas-docs/stable/reference/api/pd.read_parquet.html

        Returns:
            pd.DataFrame: parquet file

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3._read_parquet(file_name='test.parquet', encoding='utf-8')
        """
        import sys
        assert (
            "pyarrow" in sys.modules
        ), "pyarrow is not installed. if you want to read parquet file, please install pyarrow(pip install pyarrow)"
        from pyarrow import BufferReader
        
        if "encoding" in kwargs:
            kwargs.pop("encoding")

        self._read_file(file_name)
        return pd.read_parquet(BufferReader(self.read_content), **kwargs)

    def extension(self, file_name: str) -> str:
        """
        get file extension

        Args:
            file_name (str): file name (including path)

        Returns:
            str: file extension

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.extension(file_name='test.txt')
        """
        return (
            file_name.split(".")[-2]
            if (file_name.split(".")[-1] == "gz" or file_name.split(".")[-1] == "bz2")
            else file_name.split(".")[-1]
        )

    def write_csv(
        self,
        file_name: str,
        file_content: any,
        compression: str = None,
        encoding: str = "utf-8",
        **kwargs,
    ):
        """
        Write csv file to s3 bucket

        Args:
            file_name (str): file name (including path)
            file_content (any): file content
            compression (str): compression type (default: None, gzip, bz2)
            encoding (str): encoding (default: utf-8)
            **kwargs: arguments for pd.DataFrame.to_csv
                document: https://pd.pydata.org/pandas-docs/stable/reference/api/pd.DataFrame.to_csv.html

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.write_csv(file_name='test.csv', df=df, encoding='utf-8', index=False)
        """
        file_content = (
            file_content.to_csv(encoding=encoding, **kwargs)
            if isinstance(file_content, pd.DataFrame)
            else file_content
        )

        self._write_file(
            file_name=file_name,
            file_content=file_content,
            compression=compression,
            encoding=encoding,
        )

    def write_json(
        self,
        file_name: str,
        file_content: any,
        compression: str = None,
        encoding: str = "utf-8",
        **kwargs,
    ):
        """
        Write json file to s3 bucket

        Args:
            file_name (str): file name (including path)
            file_content (any): dict, list, pd.DataFrame
            compression (str): compression type (default: None, gzip, bz2)
            encoding (str): encoding (default: utf-8)
            **kwargs: arguments for pd.DataFrame.to_json
                document: https://pd.pydata.org/pandas-docs/stable/reference/api/pd.DataFrame.to_json.html

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.write_json(file_name='test.json', df=df, encoding='utf-8', force_ascii=False, orient='records', indent=4)
        """
        file_content = (
            file_content.to_json(**kwargs)
            if isinstance(file_content, pd.DataFrame)
            else file_content
        )
        file_content = (
            json.dumps(file_content, ensure_ascii=False)
            if isinstance(file_content, dict)
            else file_content
        )
        file_content = (
            json.dumps(file_content, ensure_ascii=False)
            if isinstance(file_content, list)
            else file_content
        )

        self._write_file(
            file_name=file_name,
            file_content=file_content,
            compression=compression,
            encoding=encoding,
        )

    def write_txt(
        self,
        file_name: str,
        file_content: str,
        compression: str = None,
        encoding: str = "utf-8",
    ):
        """
        Write txt file to s3 bucket

        Args:
            file_name (str): file name (including path)
            file_content (str): file content
            compression (str): compression type (default: None, gzip, bz2)
            encoding (str): encoding (default: utf-8)

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.write_txt(file_name='test.txt', file_content='test')
        """
        file_content = (
            str(file_content) if not isinstance(file_content, str) else file_content
        )

        self._write_file(
            file_name=file_name,
            file_content=file_content,
            compression=compression,
            encoding=encoding,
        )

    def write_pickle(
        self,
        file_name: str,
        file_content: any,
        compression: str = None,
        encoding: str = "utf-8",
        **kwargs,
    ):
        """
        Write pickle file to s3 bucket

        Args:
            file_name (str): file name (including path)
            file_content (any): file content
            compression (str): compression type (default: None, gzip, bz2)
            encoding (str): encoding (default: utf-8)
            **kwargs: arguments for pickle.dumps
                document: https://docs.python.org/3/library/pickle.html#pickle.dumps

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.write_pickle(file_name='test.pkl', file_content='test')
        """
        file_content = pickle.dumps(file_content, **kwargs)

        self._write_file(
            file_name=file_name,
            file_content=file_content,
            compression=compression,
            encoding=encoding,
        )

    def write_parquet(
        self,
        file_name: str,
        file_content: any,
        compression: str = None,
        encoding: str = "utf-8",
        **kwargs,
    ):
        """
        Write parquet file to s3 bucket

        Args:
            file_name (str): file name (including path)
            file_content (any): pd.DataFrame, pd.Series, bytes
            compression (str): compression type (default: None, gzip, bz2)
            encoding (str): encoding (default: utf-8)
            **kwargs: arguments for pd.DataFrame.to_parquet
                document: https://pd.pydata.org/pandas-docs/stable/reference/api/pd.DataFrame.to_parquet.html

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.write_parquet(file_name='test.parquet', file_content=df, encoding='utf-8', index=False)
        """
        file_content = (
            file_content.to_parquet(**kwargs)
            if isinstance(file_content, pd.DataFrame)
            else file_content
        )
        file_content = (
            file_content.to_parquet(**kwargs)
            if isinstance(file_content, pd.Series)
            else file_content
        )
        assert isinstance(file_content, bytes), "file_content must be bytes"

        self._write_file(
            file_name=file_name,
            file_content=file_content,
            compression=compression,
            encoding=encoding,
        )

    def make_tree(
        self, prefix: str = "", delimiter: str = "/", with_file_name: bool = False
    ) -> dict:
        """
        make tree of file path in s3 bucket(recursive)

        Args:
            prefix (str): file path prefix (default: '')
            delimiter (str): file path delimiter (default: '/')
            with_file_name (bool): include file name in tree (default: False)

        Returns:
            dict: tree of file path in s3 bucket

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.make_tree()
        """

        response_iterator = self.paginator.paginate(
            Bucket=self.__bucket, Prefix=prefix, Delimiter=delimiter
        )

        tree = {}
        for response in response_iterator:
            if "CommonPrefixes" in response: # if response has CommonPrefixes, it means it has sub directory
                for file in response["CommonPrefixes"]: 
                    tree[file["Prefix"]] = self.make_tree(
                        file["Prefix"], delimiter, with_file_name
                    )

            if with_file_name:
                if "Contents" in response:
                    for file in response["Contents"]:
                        tree[file["Key"]] = None

        return tree

    def list_files(self, prefix: str = "", delimiter: str = "") -> list:
        """
        get list of files in s3 bucket, not tree structure but simple array

        Args:
            prefix (str): file path prefix (default: '')
            delimiter (str): file path delimiter (default: '/')
        Returns:
            list: list of files in s3 bucket

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.list_files()
        """
        response_iterator = self.paginator.paginate(
            Bucket=self.__bucket, Prefix=prefix, Delimiter=delimiter
        )

        files = []
        for response in response_iterator:
            if "Contents" in response:
                for file in response["Contents"]:
                    files.append(file["Key"])

        return files

    def find_file(
        self,
        file_name: str,
        prefix: str = "",
        delimiter: str = "/",
        str_contains: bool = True,
    ) -> str:
        """
        find file in s3 bucket(recursive)

        Args:
            file_name (str): file name
            prefix (str): file path prefix (default: '')
            delimiter (str): file path delimiter (default: '/')
            str_contains (bool): if True, file_name is substring of file path (default: True)

        Returns:
            str: file path
        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.find_file('test.txt')
        """
        response_iterator = self.paginator.paginate(
            Bucket=self.__bucket, Prefix=prefix, Delimiter=delimiter
        )

        for response in response_iterator:
            if "Contents" in response:  # if response has CommonPrefixes, it means it has sub directory
                for file in response["Contents"]:
                    if str_contains:  # if file_name is substring of file path
                        if file_name in file["Key"]:
                            return file["Key"]
                    else:
                        if file_name == file["Key"]:
                            return file["Key"]

        for response in response_iterator:
            if "CommonPrefixes" in response:  # if folder exists
                for prefix in response["CommonPrefixes"]: 
                    file_path = self.find_file(
                        file_name, prefix["Prefix"], delimiter, str_contains
                    )  # recursive
                    if file_path is not None:
                        return file_path

        return None

    def find_files(self, prefix: str, delimiter: str = "/") -> list:
        """
        find files in s3 bucket(recursive)

        Args:
            prefix (str): file path prefix
            delimiter (str): file path delimiter (default: '/')

        Returns:
            list: list of file path

        Example:
            >>> s3 = 
            >>> s3.find_files(folder_path='assets/csv/')
        """
        tree = self.make_tree(prefix, delimiter, with_file_name=True)
        return [file for file in tree if tree[file] is None]

    def read_auto(
        self, file_name: str, extension: str = None, encoding: str = "utf-8", **kwargs
    ) -> object:
        """
        read file in s3 bucket automatically

        Args:
            file_name (str): file name
            extension (str): file extension (default: None)
            encoding (str): file encoding (default: 'utf-8')
            **kwargs: arguments for read_csv, read_json, read_txt, read_pkl, read_parquet

        Returns:
            object: file content

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.read_auto(file_name='test.txt', encoding='utf-8')
        """
        extension = self.extension(file_name) if extension is None else extension
        raise_error = lambda msg: raise_error(msg, "aws_s3.read_auto")
        return (
            getattr(self, f"read_{extension}")(
                file_name=file_name, encoding=encoding, **kwargs
            )
            if extension in ["csv", "json", "txt", "sql", "pkl", "parquet"]
            else raise_error(f"This extension is not supported. Please enter one of ({extension}).")
        )

    def read_thread(
        self,
        prefix: str,
        delimiter: str = "/",
        extension=None,
        str_contains: str = None,
        workers: int = 4,
        encoding: str = "utf-8",
        **kwargs,
    ) -> object:
        """
        A function that reads all files in a folder and merges them into one file (using _thread_map)
        Only files that can be concat can be used

        Args:
            prefix (str): file path (default: '')
            delimiter (str): File path delimiter (default: '/')
            extension (str): File extension (default: None)
            str_contains (str): read only files that match part of the filename (default: None)
            workers (int): number of threads (default: 4)
                - If the workers parameter is not used, set the number of threads to 4
                - It is recommended to set the thread count automatically using os.cpu_count()
            encoding (str): encoding (default: 'utf-8')
            **kwargs: arguments for Read_csv, read_json, read_txt, read_pickle, read_parquet

        Returns:
            object: file content

        Example:
            >>> s3 = s3namic(bucket, access_key, secret_key, region)
            >>> s3.read_thread(folder_path='assets/csv/', extension='csv', prefix='assets/csv/', delimiter='/', workers=4, encoding='utf-8')
        """
        files = self.find_files(prefix, delimiter)
        if str_contains is not None:
            files = [file for file in files if str_contains in file]
        return self._thread_map(
            func=lambda x: self.read_auto(
                x, extension=extension, encoding=encoding, **kwargs
            ),
            iterable=files,
            max_workers=workers,
        )
