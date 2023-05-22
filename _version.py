import json

version_json = '''
{
 "date": "2023-05-22T14:20:00+09:00",
 "error": null,
 "version": "0.0.2"
}
'''

def get_version():
    return json.loads(version_json)