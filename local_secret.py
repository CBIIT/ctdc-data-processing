from os import environ


def get_local_secrets():
    secrets = {
        "OKTA_CLIENT_ID": environ.get("OKTA_CLIENT_ID"),
        "OKTA_CLIENT_SECRET": environ.get("OKTA_CLIENT_SECRET"),
        "CURRENT_OKTA_USERNAME": environ.get("CURRENT_OKTA_USERNAME"),
        "CURRENT_OKTA_PASSWORD": environ.get("CURRENT_OKTA_PASSWORD"),
        "OKTA_GRANT_TYPE": environ.get("OKTA_GRANT_TYPE"),
        "OKTA_SCOPE": environ.get("OKTA_SCOPE")
    }
    return secrets