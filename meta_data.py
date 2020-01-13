import requests
from bento.common.utils import removeTrailingSlash

def get_patient_meta_data(token, base_url, patient_id):
    url = '{}/{}'.format(removeTrailingSlash(base_url), patient_id)
    headers = {'Authorization': token}
    result = requests.get(url, headers=headers)
    if result and result.ok:
        return result.json()
    else:
        return None
