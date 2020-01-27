import json

from bento.common.utils import get_logger

class Config:
    def __init__(self, file_name):
        self.log = get_logger('Configuration')
        # Read the Configuration File
        with open(file_name) as config_file:
            self.data = json.load(config_file)

        # Read the region
        self.region = self.data['region']
        # Get List of Arms
        self.armIds = self.data['armIds']
        if self.data['useProd'] == False:
            self.log.info('Using Match UAT Environment')
            # Get the Secret Name UAT
            self.secret_name = self.data['secretName']
            # Get Okta UAT Authorization URL
            self.oktaAuthUrl = self.data["oktaAuthUrl"]
            # Get the Match UAT Treatment Arm Api URL
            self.matchBaseUrl = self.data['matchUatBaseUrl']
            # Get the Match UAT Patient Api URL
            self.matchBaseUrlPatient = self.data['matchUatBaseUrlPatient']
            self.matchArmUrl = self.data['matchUatArmUrl']
        else:
            self.log.info('Using Match Production Environment')
            # Get the Secret Name
            self.secret_name = self.data['secretNameProd']
            # Get Okta Authorization URL
            self.oktaAuthUrl = self.data["oktaAuthUrlProd"]
            # Get the Match Treatment Arm Api URL
            self.matchBaseUrl = self.data['matchProdBaseUrl']
            # Get the Match Patient Api URL
            self.matchBaseUrlPatient = self.data['matchProdBaseUrlPatient']
            self.matchArmUrl = self.data['matchProdArmUrl']


