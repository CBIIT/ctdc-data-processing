const request = require('request');
const querystring = require('querystring');
const config = require('./config.json');

const matchBaseUrl=config.matchUatBaseUrl;

async function getMatchData(){
	//Get the Secrets from AWS Secrets Manager.
	let secrets = await require('./modules/secrets.js').getSecrets();

	//console.log(secrets);
	//Use the Secrets to get the Okta Auth Bearer Token
	let token = await require('./modules/tokens.js').getOktaToken(secrets);
	//console.log(token);

	//Get the list of Arms from the config file
	armIds = config.armIds;

	//Create a list of promises for getting List of Patients for all Arms
	//using the token obtained
	promisesList=[];
	const patients = require('./modules/patients-treatment-arm.js');
	for (id=0;id<armIds.length;id++){
	  promisesList.push(patients.getPatientsByArmPromise(armIds[id], token));
	}

	//Getting a list of Patients
	patientsList = await Promise.all(promisesList);
	console.log(patientsList);



}

getMatchData().then((res)=>{
	console.log('Done')
})

