const querystring = require('querystring');
const config = require('../config.json');
const request = require('request');

function getOktaToken(secrets){
	return new Promise((resolve, reject)=>{

		let authParams={
			client_id: secrets.OKTA_CLIENT_ID,
			username: secrets.CURRENT_OKTA_USERNAME,
			password: secrets.CURRENT_OKTA_PASSWORD,
			client_secret: secrets.OKTA_CLIENT_SECRET,
			grant_type: "password",
			scope: "openid profile"
		};
		var authParamsStr = querystring.stringify(authParams);
		
		request({
		  url: config.oktaAuthUrl,
		  method: "POST",
		  headers: {
		     'Content-Type': 'application/x-www-form-urlencoded'
		  },
		  body: authParamsStr
		  
		}, (err, res) =>{
		      if(err) {
		        console.error('Error is: '+err);
		        reject(err)
		      } else {
		      	if(res.statusCode != 200){
		      		console.log('Response Code for Token request is : '+res.statusCode)
		      		reject(res)
		      	}
		        //console.log(res);
		        resBody= JSON.parse(res.body)
		        //console.log(resBody);
		        token = resBody.token_type+' '+ resBody.access_token
		        resolve(token);
		      }

		});

	});
}
module.exports.getOktaToken=getOktaToken;