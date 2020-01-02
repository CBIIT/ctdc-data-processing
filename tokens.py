import requests

def get_okta_token(secrets,config,url=''):
    
    
    body = {"client_id": secrets["OKTA_CLIENT_ID"],
			"username": secrets["CURRENT_OKTA_USERNAME"],
			"password": secrets["CURRENT_OKTA_PASSWORD"],
			"client_secret": secrets["OKTA_CLIENT_SECRET"],
			"grant_type": "password",
			"scope": "openid profile"}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    

    r = requests.post(url, data=(body), headers=headers)
    token = r.json()['token_type'] +' '+ r.json()['access_token']
    #print(token)
    return token