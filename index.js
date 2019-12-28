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
	console.log('Token acquired!');

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
	//console.log(patientsList[0].get(armIds[0]).length);
	console.log('List of Patients received!');

	const patientFiles = require('./modules/patient-individual.js');
	//This is to store the list of promises to get the S3 URLs per Arm
	promisesFileList=[];
	//This is to store the list of files to get the S3 URLs. The files
	//are returned in a 2-D array the form FileList[armId][Array of Map Values]
	//The Map Keys are Patient Id and Values are an array of S3 URLs
	FileList=[];
	//Iterate for all Arms and all Patients within the Arm and retrieve
	//S3 URLs for all the patients for Sequencing with the status of
	//CONFIRMED in the Match System
	for (id=0;id<armIds.length;id++){
		promisesFileList=[];
		for(j=0;j<patientsList[id].get(armIds[id]).length;j++){
			//patiendId= patientsList[id].get(armIds[id])[j];
			promisesFileList.push(patientFiles.getPatientPromise(patientsList[id].get(armIds[id])[j],token,config.fileProjectionQuery));

		}
		FileList[id] = await Promise.all(promisesFileList);	
	}

	//FileList = await Promise.all(promisesFileList);
	console.log(FileList[0]);
	console.log('List of Files received!');





}

getMatchData().then((res)=>{
	console.log('Done')
})

