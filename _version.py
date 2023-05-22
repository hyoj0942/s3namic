import json

version_json = '''
{
 "date": "2023-05-22T16:15:00+09:00",
 "error": null,
 "version": "0.0.5"
}
'''

def get_version():
    return json.loads(version_json)