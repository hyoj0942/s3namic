# Python package for facilitating data management and CRUD in AWS S3

## Installation
`pip install s3namic`

---

## Import module

```python
from s3namic import s3namic
s3 = s3namic(
  bucket="bucket_name",
  access_key="access_key",
  secret_key="secret_key",
  region="region",
)
```

---

## Check S3 structure in tree form

```python
s3_tree = s3.make_tree(
    # with_file_name=True, # True if even the file name is included in the tree
    )

import json
s3_tree = json.dumps(s3_tree, indent=4, ensure_ascii=False)
print(s3_tree)
```

output:

```python
{
    "assets/": {
        "assets/backup/": {},
        "assets/batch_raw/": {
            "assets/batch_raw/batchData": {}
        },
        ...
}
```

---

## Check S3 structure in list form

```python
s3_list = s3.list_files()
print(s3_list)
```

output:

```python
['assets/json/first_file.json', 'assets/json/second_file.json', ... ]
```

### Find a specific file in s3

- ### find_file

```python
test_path = s3.find_file(file_name="2023-04-30", str_contains=True)
print(f"2023-04-30 File path containing filename: '{test_path}'")
```

output:

```python
"2023-04-30 File path containing filename: 'assets/csv/2023-04-30.csv'"
```

- ### find_files

```python
prefix_path = test_path[:-len(test_path.split("/")[-1])]
test_files = s3.find_files(prefix=prefix_path)
print(f"Number of files under '{prefix_path}': {len(test_files)}")
```

output:

```python
"Number of files under 'assets/csv/': 112"
```

---

### Get from s3 to a specific path url

```python
print(s3.get_file_url(file_name=test_path, expires_in=3600)) # Expires in 3600 seconds (1 hour)
```

output:

```python
"https://bucket_name.s3.amazonaws.com/assets/csv/test.csv"
```

---

## CRUD from S3

### C, U(upload_file, \_write_file)

- Upload files to s3
- The upload_file method reads the file and uploads it to memory, and the `_write_file` method writes the file directly without reading it, so memory usage is small.
- Use `upload_file` to upload a file in your local to the s3 bucket, and use `_write_file` to directly save a variable to s3 in the code.
- The `write_csv`, `write_json`, `write_pkl`, `write_txt`, and `write_parquet` methods call the `_write_file` method to save the file according to the extension.

### R(\_read_file)

- read file from s3
- The `read_csv`, `read_json`, `read_pkl`, `read_txt`, and `read_parquet` methods call the `_read_file` method to read files according to the extension.
- The `read_auto` method calls the above methods according to the extension to read the file.
- The `read_thread` method speeds up the read_auto method by executing it multi-threaded.

### D(delete_file)

- Delete files from s3

### examples

- This example uses the csv extension, but json, pkl, txt, and parquet extensions can be used equally (refer to the above methods for usage).

```python
import pandas as pd

# Save variable to file (write_csv)
test_write_csv = pd.DataFrame({
    "test": [
        "한글",
        "English",
        1234,
        "!@#$%^&*()_+",
        "😀👍👍🏻👍🏼"
    ]
})
# directly save the variable (dataframe)
s3.write_csv(file_name="assets/test/test_write.csv", file_content=test_write_csv, encoding="utf-8", index=False)
# Compress and save in gzip or bzip2 format
s3.write_csv(file_name="assets/test/test_write.csv.gz", file_content=test_write_csv, compression="gzip", encoding="utf-8", index=False)
s3.write_csv(file_name="assets/test/test_write.csv.bz2", file_content=test_write_csv, compression="bz2", encoding="utf-8", index=False)
```

```python
# Read the saved file (read_csv)
pd.concat([
    s3.read_csv(file_name="assets/test/test_write.csv", encoding="utf-8").rename(columns={"test": "Basic format"}),
    # Read compressed files in gzip or bzip2 format
    s3.read_csv(file_name="assets/test/test_write.csv.gz", encoding="utf-8").rename(columns={"test": "gzip format"}),
    s3.read_csv(file_name="assets/test/test_write.csv.bz2", encoding="utf-8").rename(columns={"test": "bzip2 format"})
], axis=1)
```

output:

<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Basic format</th>
      <th>gzip format</th>
      <th>bzip2 format</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>한글</td>
      <td>한글</td>
      <td>한글</td>
    </tr>
    <tr>
      <th>1</th>
      <td>English</td>
      <td>English</td>
      <td>English</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1234</td>
      <td>1234</td>
      <td>1234</td>
    </tr>
    <tr>
      <th>3</th>
      <td>!@#$%^&amp;*()_+</td>
      <td>!@#$%^&amp;*()_+</td>
      <td>!@#$%^&amp;*()_+</td>
    </tr>
    <tr>
      <th>4</th>
      <td>😀👍👍🏻👍🏼</td>
      <td>😀👍👍🏻👍🏼</td>
      <td>😀👍👍🏻👍🏼</td>
    </tr>
  </tbody>
</table>
</div>

</br>

```python
# Download the saved file locally (download_file)
load_path = os.getcwd()
s3.download_file(file_name="assets/test/test_write.csv", load_path=load_path+"/test_write.csv")
s3.download_file(file_name="assets/test/test_write.csv.gz", load_path=load_path+"/test_write.csv.gz")
s3.download_file(file_name="assets/test/test_write.csv.bz2", load_path=load_path+"/test_write.csv.bz2")
```

```python
# Delete a file on s3 (delete_file)
print(f"List of files before deletion: {s3.find_files(prefix='assets/test/')}")
s3.delete_file(file_name="assets/test/test_write.csv")
s3.delete_file(file_name="assets/test/test_write.csv.gz")
s3.delete_file(file_name="assets/test/test_write.csv.bz2")
print(f"List of files after deletion: {s3.find_files(prefix='assets/test/')}")
```

output:

```python
"List of files before deletion: ['assets/test/', 'assets/test/test.csv', 'assets/test/test.json', 'assets/test/test.parquet', 'assets/test/test.pickle', 'assets/test/test.pkl', 'assets/test/test.txt', 'assets/test/test_write.csv', 'assets/test/test_write.csv.bz2', 'assets/test/test_write.csv.gz']"
"List of files after deletion: ['assets/test/', 'assets/test/test.csv', 'assets/test/test.json', 'assets/test/test.parquet', 'assets/test/test.pickle', 'assets/test/test.pkl', 'assets/test/test.txt']"
```

</br>

```python
# Upload a file stored locally (upload_file)
print(f"List of files before upload: {s3.find_files(prefix='assets/test/')}")
s3.upload_file(file_name="assets/test/test_write.csv", file_path=load_path+"/test_write.csv")
s3.upload_file(file_name="assets/test/test_write.csv.gz", file_path=load_path+"/test_write.csv.gz")
s3.upload_file(file_name="assets/test/test_write.csv.bz2", file_path=load_path+"/test_write.csv.bz2")
print(f"List of files after upload: {s3.find_files(prefix='assets/test/')}")
```

output:

```python
"List of files before upload: ['assets/test/', 'assets/test/test.csv', 'assets/test/test.json', 'assets/test/test.parquet', 'assets/test/test.pickle', 'assets/test/test.pkl', 'assets/test/test.txt']"
"List of files after upload: ['assets/test/', 'assets/test/test.csv', 'assets/test/test.json', 'assets/test/test.parquet', 'assets/test/test.pickle', 'assets/test/test.pkl', 'assets/test/test.txt', 'assets/test/test_write.csv', 'assets/test/test_write.csv.bz2', 'assets/test/test_write.csv.gz']"
```

```python
# Delete local files
os.remove(load_path+"/test_write.csv")
os.remove(load_path+"/test_write.csv.gz")
os.remove(load_path+"/test_write.csv.bz2")
```

---

### Methods that use CRUD in various ways

- **`read_auto`**
  - A method that executes one of `read_csv`, `read_excel`, `read_json`, `read_parquet`, and `read_pkl` depending on the file extension
  - You can automatically find the extension in the file name or specify the extension with the extension argument.<br><br>
- **`read_thread`**
  - Execute the `read_auto` method with multi-threading<br><br>
- **`compress`**, **`decompress`**
  - Compress and decompress files in s3 bucket and save as files
  - Using `_read_file()` method, `_write_file()` method<br><br>

<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>test</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>한글</td>
    </tr>
    <tr>
      <th>1</th>
      <td>English</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1234</td>
    </tr>
    <tr>
      <th>3</th>
      <td>!@#$%^&amp;*()_+</td>
    </tr>
    <tr>
      <th>4</th>
      <td>😀👍👍🏻👍🏼</td>
    </tr>
  </tbody>
</table>
</div>

```python
auto_path = s3.find_file(file_name="2022-12", str_contains=True)  # File path with filename containing 2022-12
print(f"File path with filename containing 2023-04-30: {auto_path}")
# Just put the folder path as prefix
folder_path = auto_path[:auto_path.rfind('/')] + '/'
print(f"Folder path of the file path: {folder_path}")
print(f"Number of files in the folder: {len(s3.find_files(prefix=folder_path))}")
auto_file = s3.read_thread(prefix=folder_path, encoding="cp949", workers=os.cpu_count(), extension="csv")
print(f"Number of data frames of files in the folder (list type): {len(auto_file)}")
```

output:

```python
"File path with filename containing 2023-04-30: assets/csv/2023-04-30.csv"
"Folder path of the file path: assets/csv/"
"Number of files in the folder: 112"
"Number of data frames of files in the folder (list type): 112"
```

</br>

```python
s3.compress(file_name="assets/test/test_write.csv", compression="gzip")
s3.compress(file_name="assets/test/test_write.csv", compression="bz2")
s3.decompress(file_name="assets/test/test_write.csv.gz")
s3.decompress(file_name="assets/test/test_write.csv.bz2")
```

output:

```python
"The file assets/test/test_write.csv was compressed using gzip and saved as assets/test/test_write.csv.gz."
"The file assets/test/test_write.csv was compressed using bz2 and saved as assets/test/test_write.csv.bz2."
"The file assets/test/test_write.csv.gz was unzipped and saved as assets/test/test_write.csv."
"The file assets/test/test_write.csv.bz2 was unzipped and saved as assets/test/test_write.csv"
```
