import json

from bento.common.utils import get_logger, removeTrailingSlash

class Arm:
    def __init__(self, obj):
        self.arm_id = obj['armId']
        self.phs_id = obj['phsId']
        self.pubmed_id = obj['pubmedId']
        self.bucket_name = obj['bucketName']
        self.ctdcArmId = obj['ctdcArmId']
        self.clinicalTrialID = obj['clinicalTrialID']

class Config:
    def __init__(self, file_name):
        self.log = get_logger('Configuration')
        # Read the Configuration File
        with open(file_name) as config_file:
            self.data = json.load(config_file)

        # Read the region
        self.region = self.data['region']
        self.domain = self.data['domain']

        # Read arm objects
        self.arms = []
        for obj in self.data['arms']:
            arm_obj = {
                'phsId': obj['phsId'],
                'pubmedId': obj['pubmedId'],
                'bucketName': obj['bucketName'],
                'ctdcArmId': obj['ctdcArmId'],
                'clinicalTrialID': self.data['clinicalTrialID']
            }
            for armId in obj['matchArms']:
                arm_obj['armId'] = armId
                self.arms.append(Arm(arm_obj))

        # Get List of Arms
        self.meta_data_path = self.data['metaDataPath']
        self.meta_data_bucket = self.data['metaDataBucket']
        self.cipher_key = self.data['cipher_key']
        self.use_prod = self.data['useProd']

        # Get the Secret Name UAT
        self.secret_name = self.data['secretName']
        # Get Okta UAT Authorization URL
        self.okta_auth_url = self.data["oktaAuthUrl"]
        # Get the Match UAT Treatment Arm Api URL
        self.match_base_url = removeTrailingSlash(self.data['matchBaseUrl'])
        # Get the blind ID Mapping file
        self.blindID_mapping_file = self.data['blindID_mapping_file']

        if self.use_prod == False:
            self.log.info('Using Match UAT Environment')
        else:
            self.log.info('Using Match Production Environment')
